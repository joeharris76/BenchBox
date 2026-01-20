"""Analytics tools for BenchBox MCP server.

Provides tools for query plan analysis and regression detection.

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

from mcp.server.fastmcp import FastMCP
from mcp.types import ToolAnnotations

from benchbox.mcp.errors import ErrorCode, make_error, make_not_found_error

logger = logging.getLogger(__name__)

# Tool annotations for read-only analytics tools
ANALYTICS_READONLY_ANNOTATIONS = ToolAnnotations(
    title="Read-only analytics tool",
    readOnlyHint=True,
    destructiveHint=False,
    idempotentHint=True,
    openWorldHint=False,
)

# Default results directory
DEFAULT_RESULTS_DIR = Path("benchmark_runs/results")


def register_analytics_tools(mcp: FastMCP) -> None:
    """Register analytics tools with the MCP server.

    Args:
        mcp: The FastMCP server instance to register tools with.
    """

    @mcp.tool(annotations=ANALYTICS_READONLY_ANNOTATIONS)
    def get_query_plan(
        result_file: str,
        query_id: str,
        format: str = "tree",
    ) -> dict[str, Any]:
        """Get query execution plan from benchmark results.

        Retrieves the query plan for a specific query from a benchmark run.
        Query plans must have been captured using --capture-plans during benchmark execution.

        Args:
            result_file: Name of the result file containing query plans
            query_id: Query identifier (e.g., '1', 'Q1', 'q05')
            format: Output format ('tree', 'json', or 'summary')

        Returns:
            Query plan in the requested format.

        Example:
            get_query_plan(result_file="run.json", query_id="5")
            get_query_plan(result_file="run.json", query_id="Q1", format="summary")
        """
        # Validate format
        valid_formats = ["tree", "json", "summary"]
        format_lower = format.lower()
        if format_lower not in valid_formats:
            return make_error(
                ErrorCode.VALIDATION_INVALID_FORMAT,
                f"Invalid format: {format}",
                details={"valid_formats": valid_formats},
                suggestion=f"Use one of: {', '.join(valid_formats)}",
            )

        # Load the results file
        results_dir = DEFAULT_RESULTS_DIR
        file_path = results_dir / result_file

        if not file_path.exists():
            if not result_file.endswith(".json"):
                file_path = results_dir / (result_file + ".json")

        if not file_path.exists():
            return make_error(
                ErrorCode.RESOURCE_NOT_FOUND,
                f"Result file not found: {result_file}",
                details={"requested_file": result_file},
                suggestion="Use list_recent_runs() to see available result files",
            )

        try:
            with open(file_path) as f:
                data = json.load(f)

            # Normalize query ID
            normalized_id = query_id.upper().lstrip("Q")
            search_ids = [query_id, normalized_id, f"Q{normalized_id}", f"q{normalized_id}"]

            # Search for query execution across all phases
            query_exec = None
            found_phase = None
            for phase_name, phase_data in data.get("phases", {}).items():
                for query_result in phase_data.get("queries", []):
                    qid = query_result.get("query_id", "")
                    if qid in search_ids or str(qid) in search_ids:
                        query_exec = query_result
                        found_phase = phase_name
                        break
                if query_exec:
                    break

            if not query_exec:
                # List available query IDs
                available_ids = []
                for phase_data in data.get("phases", {}).values():
                    for q in phase_data.get("queries", []):
                        if "query_id" in q:
                            available_ids.append(str(q["query_id"]))

                return make_not_found_error(
                    "query",
                    query_id,
                    available=sorted(set(available_ids))[:20],
                )

            # Check if query plan was captured
            query_plan = query_exec.get("query_plan")
            if not query_plan:
                return {
                    "status": "no_plan",
                    "query_id": query_id,
                    "phase": found_phase,
                    "message": "No query plan captured for this query",
                    "suggestion": "Run benchmark with --capture-plans flag to capture query plans",
                    "query_info": {
                        "runtime_ms": query_exec.get("runtime_ms"),
                        "status": query_exec.get("status"),
                    },
                }

            # Format the query plan
            response: dict[str, Any] = {
                "status": "success",
                "query_id": query_id,
                "phase": found_phase,
                "runtime_ms": query_exec.get("runtime_ms"),
            }

            if format_lower == "json":
                response["plan"] = query_plan
            elif format_lower == "summary":
                # Extract summary statistics from plan
                response["summary"] = _extract_plan_summary(query_plan)
            else:  # tree
                # Convert to readable tree format
                response["plan_tree"] = _format_plan_tree(query_plan)

            return response

        except json.JSONDecodeError as e:
            return make_error(
                ErrorCode.RESOURCE_INVALID_FORMAT,
                f"Invalid JSON in result file: {e}",
                details={"file": result_file, "parse_error": str(e)},
            )
        except Exception as e:
            logger.exception(f"Failed to get query plan: {e}")
            return make_error(
                ErrorCode.INTERNAL_ERROR,
                f"Failed to get query plan: {e}",
                details={"exception_type": type(e).__name__},
            )

    @mcp.tool(annotations=ANALYTICS_READONLY_ANNOTATIONS)
    def detect_regressions(
        platform: str | None = None,
        benchmark: str | None = None,
        threshold_percent: float = 10.0,
        lookback_runs: int = 5,
    ) -> dict[str, Any]:
        """Automatically detect performance regressions across recent runs.

        Compares recent benchmark runs to identify queries that have regressed.
        Finds the two most recent comparable runs and analyzes differences.

        Args:
            platform: Filter by platform name (optional)
            benchmark: Filter by benchmark name (optional)
            threshold_percent: Percentage change threshold for regression (default: 10%)
            lookback_runs: Number of recent runs to consider (default: 5)

        Returns:
            Regression analysis with affected queries and recommendations.

        Example:
            detect_regressions()  # Check all recent runs
            detect_regressions(platform="duckdb", benchmark="tpch")
            detect_regressions(threshold_percent=5, lookback_runs=10)
        """
        results_dir = DEFAULT_RESULTS_DIR

        if not results_dir.exists():
            return {
                "status": "no_data",
                "message": f"No results directory found at {results_dir}",
                "regressions": [],
            }

        # Find recent result files
        result_files = list(results_dir.glob("*.json"))
        if len(result_files) < 2:
            return {
                "status": "insufficient_data",
                "message": f"Need at least 2 benchmark runs for comparison, found {len(result_files)}",
                "regressions": [],
            }

        # Sort by modification time (newest first)
        result_files = sorted(result_files, key=lambda p: p.stat().st_mtime, reverse=True)

        # Load and filter results
        runs: list[dict[str, Any]] = []
        for file_path in result_files[: lookback_runs * 2]:  # Check extra files for filtering
            try:
                with open(file_path) as f:
                    data = json.load(f)

                run_platform = data.get("platform", {}).get("type", "unknown")
                run_benchmark = data.get("benchmark", "unknown")

                # Apply filters
                if platform and platform.lower() not in run_platform.lower():
                    continue
                if benchmark and benchmark.lower() not in run_benchmark.lower():
                    continue

                runs.append(
                    {
                        "file": file_path.name,
                        "path": str(file_path),
                        "platform": run_platform,
                        "benchmark": run_benchmark,
                        "scale_factor": data.get("scale_factor"),
                        "timestamp": data.get("timestamp", file_path.stat().st_mtime),
                        "data": data,
                    }
                )

                if len(runs) >= lookback_runs:
                    break

            except Exception as e:
                logger.warning(f"Could not parse result file {file_path}: {e}")
                continue

        if len(runs) < 2:
            return {
                "status": "insufficient_data",
                "message": f"Need at least 2 comparable runs, found {len(runs)} matching filters",
                "filters_applied": {
                    "platform": platform,
                    "benchmark": benchmark,
                },
                "regressions": [],
            }

        # Compare the two most recent runs
        newer_run = runs[0]
        older_run = runs[1]

        # Extract query timings
        def extract_timings(run_data: dict) -> dict[str, float]:
            timings = {}
            for phase_data in run_data.get("phases", {}).values():
                for query in phase_data.get("queries", []):
                    qid = str(query.get("query_id", ""))
                    runtime = query.get("runtime_ms")
                    if qid and runtime is not None:
                        timings[qid] = float(runtime)
            return timings

        older_timings = extract_timings(older_run["data"])
        newer_timings = extract_timings(newer_run["data"])

        # Detect regressions
        regressions: list[dict[str, Any]] = []
        improvements: list[dict[str, Any]] = []
        stable: list[str] = []

        all_queries = set(older_timings.keys()) | set(newer_timings.keys())
        for qid in sorted(all_queries):
            old_time = older_timings.get(qid)
            new_time = newer_timings.get(qid)

            if old_time is not None and new_time is not None and old_time > 0:
                delta_ms = new_time - old_time
                delta_pct = (delta_ms / old_time) * 100

                if delta_pct > threshold_percent:
                    regressions.append(
                        {
                            "query_id": qid,
                            "baseline_ms": round(old_time, 2),
                            "current_ms": round(new_time, 2),
                            "delta_ms": round(delta_ms, 2),
                            "delta_percent": round(delta_pct, 1),
                            "severity": _classify_regression_severity(delta_pct),
                        }
                    )
                elif delta_pct < -threshold_percent:
                    improvements.append(
                        {
                            "query_id": qid,
                            "baseline_ms": round(old_time, 2),
                            "current_ms": round(new_time, 2),
                            "delta_ms": round(delta_ms, 2),
                            "delta_percent": round(delta_pct, 1),
                        }
                    )
                else:
                    stable.append(qid)

        # Sort regressions by severity
        regressions.sort(key=lambda r: r["delta_percent"], reverse=True)

        # Calculate total runtime delta
        total_old = sum(older_timings.values())
        total_new = sum(newer_timings.values())
        total_delta_pct = ((total_new - total_old) / total_old * 100) if total_old > 0 else 0

        return {
            "status": "completed",
            "comparison": {
                "baseline": {
                    "file": older_run["file"],
                    "platform": older_run["platform"],
                    "benchmark": older_run["benchmark"],
                    "timestamp": older_run["timestamp"],
                },
                "current": {
                    "file": newer_run["file"],
                    "platform": newer_run["platform"],
                    "benchmark": newer_run["benchmark"],
                    "timestamp": newer_run["timestamp"],
                },
            },
            "summary": {
                "total_queries": len(all_queries),
                "regressions": len(regressions),
                "improvements": len(improvements),
                "stable": len(stable),
                "total_runtime_delta_percent": round(total_delta_pct, 1),
                "threshold_percent": threshold_percent,
            },
            "regressions": regressions,
            "improvements": improvements[:5],  # Top 5 improvements
            "recommendations": _generate_regression_recommendations(regressions, threshold_percent),
        }


def _extract_plan_summary(plan: dict) -> dict[str, Any]:
    """Extract summary statistics from a query plan."""
    summary = {
        "operator_count": 0,
        "estimated_rows": None,
        "estimated_cost": None,
        "join_count": 0,
        "scan_count": 0,
    }

    def count_operators(node: dict | list) -> None:
        if isinstance(node, dict):
            summary["operator_count"] += 1
            op_type = node.get("type", node.get("operator", "")).lower()
            if "join" in op_type:
                summary["join_count"] += 1
            if "scan" in op_type or "read" in op_type:
                summary["scan_count"] += 1
            if "rows" in node:
                if summary["estimated_rows"] is None:
                    summary["estimated_rows"] = node["rows"]
            if "cost" in node:
                if summary["estimated_cost"] is None:
                    summary["estimated_cost"] = node["cost"]
            for v in node.values():
                count_operators(v)
        elif isinstance(node, list):
            for item in node:
                count_operators(item)

    count_operators(plan)
    return summary


def _format_plan_tree(plan: dict, indent: int = 0) -> str:
    """Format a query plan as a readable tree string."""
    lines = []
    prefix = "  " * indent

    if isinstance(plan, dict):
        op_type = plan.get("type") or plan.get("operator") or plan.get("name") or "Node"
        lines.append(f"{prefix}├── {op_type}")

        # Add key properties
        for key in ["table", "alias", "condition", "rows", "cost"]:
            if key in plan:
                lines.append(f"{prefix}│   {key}: {plan[key]}")

        # Recurse into children
        children = plan.get("children") or plan.get("inputs") or plan.get("plans") or []
        if isinstance(children, list):
            for child in children:
                lines.append(_format_plan_tree(child, indent + 1))
        elif isinstance(children, dict):
            lines.append(_format_plan_tree(children, indent + 1))

    return "\n".join(lines)


def _classify_regression_severity(delta_pct: float) -> str:
    """Classify regression severity based on percentage change."""
    if delta_pct >= 100:
        return "critical"
    elif delta_pct >= 50:
        return "high"
    elif delta_pct >= 25:
        return "medium"
    else:
        return "low"


def _generate_regression_recommendations(regressions: list[dict], threshold: float) -> list[str]:
    """Generate recommendations based on detected regressions."""
    recommendations = []

    if not regressions:
        recommendations.append("No regressions detected above threshold - performance is stable")
        return recommendations

    critical = [r for r in regressions if r["severity"] == "critical"]
    high = [r for r in regressions if r["severity"] == "high"]

    if critical:
        recommendations.append(f"CRITICAL: {len(critical)} queries regressed >100% - investigate immediately")
        recommendations.append(f"Most affected: {', '.join(r['query_id'] for r in critical[:3])}")

    if high:
        recommendations.append(f"HIGH: {len(high)} queries regressed 50-100% - review query plans")

    if len(regressions) > 5:
        recommendations.append("Multiple regressions detected - consider reviewing recent changes")

    recommendations.append("Use get_query_plan() to analyze specific query execution plans")
    recommendations.append("Compare results with compare_results() for detailed query-level analysis")

    return recommendations


def register_historical_tools(mcp: FastMCP) -> None:
    """Register historical analysis tools with the MCP server.

    Args:
        mcp: The FastMCP server instance to register tools with.
    """
    from datetime import datetime

    @mcp.tool(annotations=ANALYTICS_READONLY_ANNOTATIONS)
    def get_performance_trends(
        platform: str | None = None,
        benchmark: str | None = None,
        metric: str = "geometric_mean",
        limit: int = 10,
    ) -> dict[str, Any]:
        """Get performance trends over multiple benchmark runs.

        Analyzes historical benchmark results to show performance trends over time.
        Useful for tracking improvements or regressions across releases.

        Args:
            platform: Filter by platform name (optional)
            benchmark: Filter by benchmark name (optional)
            metric: Performance metric ('geometric_mean', 'p50', 'p95', 'p99', 'total_time')
            limit: Maximum number of runs to analyze (default: 10)

        Returns:
            Time-series performance data for trend analysis.

        Example:
            get_performance_trends(platform="duckdb", benchmark="tpch")
            get_performance_trends(metric="p95", limit=20)
        """
        # Validate metric
        valid_metrics = ["geometric_mean", "p50", "p95", "p99", "total_time"]
        metric_lower = metric.lower()
        if metric_lower not in valid_metrics:
            return make_error(
                ErrorCode.VALIDATION_ERROR,
                f"Invalid metric: {metric}",
                details={"valid_metrics": valid_metrics},
                suggestion=f"Use one of: {', '.join(valid_metrics)}",
            )

        results_dir = DEFAULT_RESULTS_DIR
        if not results_dir.exists():
            return {
                "status": "no_data",
                "message": f"No results directory found at {results_dir}",
                "trends": [],
            }

        # Find and load result files
        result_files = list(results_dir.glob("*.json"))
        result_files = sorted(result_files, key=lambda p: p.stat().st_mtime, reverse=True)

        runs: list[dict[str, Any]] = []
        for file_path in result_files:
            if len(runs) >= limit:
                break

            try:
                with open(file_path) as f:
                    data = json.load(f)

                run_platform = data.get("platform", {}).get("type", "unknown")
                run_benchmark = data.get("benchmark", "unknown")

                # Apply filters
                if platform and platform.lower() not in run_platform.lower():
                    continue
                if benchmark and benchmark.lower() not in run_benchmark.lower():
                    continue

                # Extract query timings
                timings: list[float] = []
                for phase_data in data.get("phases", {}).values():
                    for query in phase_data.get("queries", []):
                        runtime = query.get("runtime_ms")
                        if runtime is not None and runtime > 0:
                            timings.append(float(runtime))

                if not timings:
                    continue

                # Calculate requested metric
                metric_value = _calculate_metric(timings, metric_lower)

                # Parse timestamp
                timestamp = data.get("timestamp")
                if timestamp:
                    try:
                        if isinstance(timestamp, str):
                            ts = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
                        else:
                            ts = datetime.fromtimestamp(timestamp)
                        timestamp_str = ts.isoformat()
                    except Exception:
                        timestamp_str = str(timestamp)
                else:
                    timestamp_str = datetime.fromtimestamp(file_path.stat().st_mtime).isoformat()

                runs.append(
                    {
                        "file": file_path.name,
                        "platform": run_platform,
                        "benchmark": run_benchmark,
                        "scale_factor": data.get("scale_factor"),
                        "timestamp": timestamp_str,
                        "query_count": len(timings),
                        "metric": metric_lower,
                        "value": round(metric_value, 2),
                    }
                )

            except Exception as e:
                logger.warning(f"Could not parse result file {file_path}: {e}")
                continue

        if not runs:
            return {
                "status": "no_matching_data",
                "message": "No benchmark runs match the specified filters",
                "filters_applied": {
                    "platform": platform,
                    "benchmark": benchmark,
                },
                "trends": [],
            }

        # Reverse to show oldest first (chronological order)
        runs.reverse()

        # Calculate trend direction
        if len(runs) >= 2:
            first_value = runs[0]["value"]
            last_value = runs[-1]["value"]
            if first_value > 0:
                trend_pct = ((last_value - first_value) / first_value) * 100
                if trend_pct < -5:
                    trend_direction = "improving"
                elif trend_pct > 5:
                    trend_direction = "degrading"
                else:
                    trend_direction = "stable"
            else:
                trend_direction = "unknown"
                trend_pct = 0
        else:
            trend_direction = "insufficient_data"
            trend_pct = 0

        return {
            "status": "success",
            "metric": metric_lower,
            "filters_applied": {
                "platform": platform,
                "benchmark": benchmark,
                "limit": limit,
            },
            "summary": {
                "run_count": len(runs),
                "first_run": runs[0]["timestamp"] if runs else None,
                "last_run": runs[-1]["timestamp"] if runs else None,
                "trend_direction": trend_direction,
                "trend_percent": round(trend_pct, 1),
            },
            "data_points": runs,
        }

    @mcp.tool(annotations=ANALYTICS_READONLY_ANNOTATIONS)
    def aggregate_results(
        platform: str | None = None,
        benchmark: str | None = None,
        group_by: str = "platform",
    ) -> dict[str, Any]:
        """Aggregate multiple benchmark results into summary statistics.

        Groups benchmark results by platform, benchmark, or date and calculates
        statistical summaries including mean, standard deviation, min, and max.

        Args:
            platform: Filter by platform name (optional)
            benchmark: Filter by benchmark name (optional)
            group_by: Grouping dimension ('platform', 'benchmark', or 'date')

        Returns:
            Statistical summaries grouped by the specified dimension.

        Example:
            aggregate_results()  # Group by platform
            aggregate_results(group_by="benchmark")
            aggregate_results(platform="duckdb", group_by="date")
        """
        # Validate group_by
        valid_group_by = ["platform", "benchmark", "date"]
        group_by_lower = group_by.lower()
        if group_by_lower not in valid_group_by:
            return make_error(
                ErrorCode.VALIDATION_ERROR,
                f"Invalid group_by: {group_by}",
                details={"valid_options": valid_group_by},
                suggestion=f"Use one of: {', '.join(valid_group_by)}",
            )

        results_dir = DEFAULT_RESULTS_DIR
        if not results_dir.exists():
            return {
                "status": "no_data",
                "message": f"No results directory found at {results_dir}",
                "aggregates": {},
            }

        # Find and load result files
        result_files = list(results_dir.glob("*.json"))

        # Group data
        groups: dict[str, list[dict[str, Any]]] = {}

        for file_path in result_files:
            try:
                with open(file_path) as f:
                    data = json.load(f)

                run_platform = data.get("platform", {}).get("type", "unknown")
                run_benchmark = data.get("benchmark", "unknown")

                # Apply filters
                if platform and platform.lower() not in run_platform.lower():
                    continue
                if benchmark and benchmark.lower() not in run_benchmark.lower():
                    continue

                # Determine group key
                if group_by_lower == "platform":
                    group_key = run_platform
                elif group_by_lower == "benchmark":
                    group_key = run_benchmark
                else:  # date
                    timestamp = data.get("timestamp", file_path.stat().st_mtime)
                    if isinstance(timestamp, str):
                        try:
                            ts = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
                            group_key = ts.strftime("%Y-%m-%d")
                        except Exception:
                            group_key = timestamp[:10] if len(timestamp) >= 10 else "unknown"
                    else:
                        group_key = datetime.fromtimestamp(timestamp).strftime("%Y-%m-%d")

                # Extract performance data
                timings: list[float] = []
                for phase_data in data.get("phases", {}).values():
                    for query in phase_data.get("queries", []):
                        runtime = query.get("runtime_ms")
                        if runtime is not None and runtime > 0:
                            timings.append(float(runtime))

                if not timings:
                    continue

                if group_key not in groups:
                    groups[group_key] = []

                groups[group_key].append(
                    {
                        "file": file_path.name,
                        "timings": timings,
                        "total_time": sum(timings),
                        "query_count": len(timings),
                        "scale_factor": data.get("scale_factor"),
                    }
                )

            except Exception as e:
                logger.warning(f"Could not parse result file {file_path}: {e}")
                continue

        if not groups:
            return {
                "status": "no_matching_data",
                "message": "No benchmark runs match the specified filters",
                "filters_applied": {
                    "platform": platform,
                    "benchmark": benchmark,
                },
                "aggregates": {},
            }

        # Calculate aggregates for each group
        aggregates: dict[str, dict[str, Any]] = {}
        for group_key, runs in sorted(groups.items()):
            all_timings = [t for run in runs for t in run["timings"]]
            total_times = [run["total_time"] for run in runs]

            aggregates[group_key] = {
                "run_count": len(runs),
                "total_queries": len(all_timings),
                "query_stats": {
                    "mean_ms": round(sum(all_timings) / len(all_timings), 2) if all_timings else 0,
                    "std_ms": round(_std_dev(all_timings), 2) if len(all_timings) > 1 else 0,
                    "min_ms": round(min(all_timings), 2) if all_timings else 0,
                    "max_ms": round(max(all_timings), 2) if all_timings else 0,
                    "p50_ms": round(_percentile(all_timings, 50), 2) if all_timings else 0,
                    "p95_ms": round(_percentile(all_timings, 95), 2) if all_timings else 0,
                },
                "run_stats": {
                    "mean_total_ms": round(sum(total_times) / len(total_times), 2) if total_times else 0,
                    "std_total_ms": round(_std_dev(total_times), 2) if len(total_times) > 1 else 0,
                    "min_total_ms": round(min(total_times), 2) if total_times else 0,
                    "max_total_ms": round(max(total_times), 2) if total_times else 0,
                },
                "files": [run["file"] for run in runs],
            }

        return {
            "status": "success",
            "group_by": group_by_lower,
            "filters_applied": {
                "platform": platform,
                "benchmark": benchmark,
            },
            "summary": {
                "total_groups": len(aggregates),
                "total_runs": sum(a["run_count"] for a in aggregates.values()),
            },
            "aggregates": aggregates,
        }


def _calculate_metric(timings: list[float], metric: str) -> float:
    """Calculate the specified performance metric from query timings."""
    import math

    if not timings:
        return 0

    if metric == "geometric_mean":
        # Geometric mean for performance data
        log_sum = sum(math.log(t) for t in timings if t > 0)
        return math.exp(log_sum / len(timings)) if timings else 0
    elif metric == "p50":
        return _percentile(timings, 50)
    elif metric == "p95":
        return _percentile(timings, 95)
    elif metric == "p99":
        return _percentile(timings, 99)
    elif metric == "total_time":
        return sum(timings)
    else:
        return sum(timings) / len(timings)  # Default to mean


def _percentile(data: list[float], p: float) -> float:
    """Calculate the p-th percentile of the data."""
    if not data:
        return 0
    sorted_data = sorted(data)
    k = (len(sorted_data) - 1) * (p / 100)
    f = int(k)
    c = f + 1 if f + 1 < len(sorted_data) else f
    if f == c:
        return sorted_data[f]
    return sorted_data[f] * (c - k) + sorted_data[c] * (k - f)


def _std_dev(data: list[float]) -> float:
    """Calculate standard deviation."""
    import math

    if len(data) < 2:
        return 0
    mean = sum(data) / len(data)
    variance = sum((x - mean) ** 2 for x in data) / (len(data) - 1)
    return math.sqrt(variance)
