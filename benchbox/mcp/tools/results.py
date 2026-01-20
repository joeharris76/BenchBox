"""Results tools for BenchBox MCP server.

Provides tools for retrieving, comparing, and exporting benchmark results.

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

from benchbox.mcp.errors import ErrorCode, make_error

logger = logging.getLogger(__name__)

# Tool annotations for read-only results tools
RESULTS_READONLY_ANNOTATIONS = ToolAnnotations(
    title="Read benchmark results",
    readOnlyHint=True,
    destructiveHint=False,
    idempotentHint=True,
    openWorldHint=False,
)


# Default results directory
DEFAULT_RESULTS_DIR = Path("benchmark_runs/results")


def _get_results_impl(result_file: str, include_queries: bool = True) -> dict[str, Any]:
    """Core implementation for getting benchmark results.

    This is extracted to allow reuse by other tools without MCP wrapper issues.
    """
    results_dir = DEFAULT_RESULTS_DIR
    file_path = results_dir / result_file

    if not file_path.exists():
        # Try without directory prefix
        if not result_file.endswith(".json"):
            result_file += ".json"
        file_path = results_dir / result_file

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

        response: dict[str, Any] = {
            "file": file_path.name,
            "platform": data.get("platform", {}),
            "benchmark": data.get("benchmark"),
            "scale_factor": data.get("scale_factor"),
            "timestamp": data.get("timestamp"),
            "execution_id": data.get("execution_id"),
            "system": data.get("system", {}),
        }

        # Add summary
        if "summary" in data:
            response["summary"] = data["summary"]

        # Add query results if requested
        if include_queries and "phases" in data:
            query_results = []
            for phase_name, phase_data in data.get("phases", {}).items():
                if "queries" in phase_data:
                    for query in phase_data["queries"]:
                        query_results.append(
                            {
                                "phase": phase_name,
                                "query_id": query.get("query_id"),
                                "runtime_ms": query.get("runtime_ms"),
                                "status": query.get("status"),
                            }
                        )
            response["queries"] = query_results
            response["query_count"] = len(query_results)

        return response

    except json.JSONDecodeError as e:
        return make_error(
            ErrorCode.RESOURCE_INVALID_FORMAT,
            f"Invalid JSON in result file: {e}",
            details={"file": result_file, "parse_error": str(e)},
        )
    except Exception as e:
        return make_error(
            ErrorCode.INTERNAL_ERROR,
            f"Could not read result file: {e}",
            details={"file": result_file, "exception_type": type(e).__name__},
        )


def register_results_tools(mcp: FastMCP) -> None:
    """Register results tools with the MCP server.

    Args:
        mcp: The FastMCP server instance to register tools with.
    """

    @mcp.tool(annotations=RESULTS_READONLY_ANNOTATIONS)
    def list_recent_runs(
        limit: int = 10,
        platform: str | None = None,
        benchmark: str | None = None,
    ) -> dict[str, Any]:
        """List recent benchmark runs.

        Searches for benchmark result files and returns metadata about recent runs.

        Args:
            limit: Maximum number of results to return (default: 10)
            platform: Filter by platform name (optional)
            benchmark: Filter by benchmark name (optional)

        Returns:
            List of recent benchmark runs with metadata.

        Example:
            list_recent_runs(limit=5, platform="duckdb")
        """
        results_dir = DEFAULT_RESULTS_DIR

        if not results_dir.exists():
            return {
                "runs": [],
                "count": 0,
                "message": f"No results directory found at {results_dir}",
            }

        # Find all JSON result files
        result_files = list(results_dir.glob("*.json"))

        runs = []
        for file_path in sorted(result_files, key=lambda p: p.stat().st_mtime, reverse=True):
            try:
                with open(file_path) as f:
                    data = json.load(f)

                # Extract metadata
                run_platform = data.get("platform", {}).get("type", "unknown")
                run_benchmark = data.get("benchmark", "unknown")

                # Apply filters
                if platform and platform.lower() not in run_platform.lower():
                    continue
                if benchmark and benchmark.lower() not in run_benchmark.lower():
                    continue

                run_info = {
                    "file": file_path.name,
                    "platform": run_platform,
                    "benchmark": run_benchmark,
                    "scale_factor": data.get("scale_factor", "unknown"),
                    "timestamp": data.get("timestamp", file_path.stat().st_mtime),
                    "execution_id": data.get("execution_id", "unknown"),
                }

                # Add summary metrics if available
                if "summary" in data:
                    run_info["summary"] = {
                        "total_queries": data["summary"].get("total_queries"),
                        "total_runtime_ms": data["summary"].get("total_runtime_ms"),
                    }

                runs.append(run_info)

                if len(runs) >= limit:
                    break

            except Exception as e:
                logger.warning(f"Could not parse result file {file_path}: {e}")
                continue

        return {
            "runs": runs,
            "count": len(runs),
            "total_available": len(result_files),
            "filters_applied": {
                "platform": platform,
                "benchmark": benchmark,
                "limit": limit,
            },
        }

    @mcp.tool(annotations=RESULTS_READONLY_ANNOTATIONS)
    def get_results(
        result_file: str,
        include_queries: bool = True,
    ) -> dict[str, Any]:
        """Get detailed results from a benchmark run.

        Args:
            result_file: Name of the result file (from list_recent_runs)
            include_queries: Whether to include per-query details (default: True)

        Returns:
            Full benchmark results including configuration and query timings.

        Example:
            get_results(result_file="tpch_sf001_duckdb_20231201_120000.json")
        """
        return _get_results_impl(result_file, include_queries)

    @mcp.tool(annotations=RESULTS_READONLY_ANNOTATIONS)
    def compare_results(
        file1: str,
        file2: str,
        threshold_percent: float = 10.0,
    ) -> dict[str, Any]:
        """Compare two benchmark runs to identify performance changes.

        Compares query execution times between two runs and highlights
        regressions (slowdowns) and improvements.

        Args:
            file1: First result file (baseline)
            file2: Second result file (comparison)
            threshold_percent: Percentage change threshold for highlighting (default: 10%)

        Returns:
            Comparison results with per-query deltas and summary.

        Example:
            compare_results(file1="run1.json", file2="run2.json", threshold_percent=5)
        """
        results_dir = DEFAULT_RESULTS_DIR

        # Load both files
        def load_result(filename: str) -> dict | None:
            path = results_dir / filename
            if not path.exists() and not filename.endswith(".json"):
                path = results_dir / (filename + ".json")
            if not path.exists():
                return None
            with open(path) as f:
                return json.load(f)

        data1 = load_result(file1)
        data2 = load_result(file2)

        if data1 is None:
            return make_error(
                ErrorCode.RESOURCE_NOT_FOUND,
                f"Baseline file not found: {file1}",
                details={"file_type": "baseline", "requested_file": file1},
                suggestion="Use list_recent_runs() to see available result files",
            )
        if data2 is None:
            return make_error(
                ErrorCode.RESOURCE_NOT_FOUND,
                f"Comparison file not found: {file2}",
                details={"file_type": "comparison", "requested_file": file2},
                suggestion="Use list_recent_runs() to see available result files",
            )

        # Extract query timings
        def extract_timings(data: dict) -> dict[str, float]:
            timings = {}
            for phase_data in data.get("phases", {}).values():
                for query in phase_data.get("queries", []):
                    qid = query.get("query_id")
                    runtime = query.get("runtime_ms")
                    if qid and runtime is not None:
                        timings[qid] = runtime
            return timings

        timings1 = extract_timings(data1)
        timings2 = extract_timings(data2)

        # Compare queries
        all_queries = set(timings1.keys()) | set(timings2.keys())
        comparisons = []
        regressions = []
        improvements = []

        for qid in sorted(all_queries):
            t1 = timings1.get(qid)
            t2 = timings2.get(qid)

            comp = {"query_id": qid, "baseline_ms": t1, "comparison_ms": t2}

            if t1 is not None and t2 is not None and t1 > 0:
                delta_ms = t2 - t1
                delta_pct = (delta_ms / t1) * 100
                comp["delta_ms"] = round(delta_ms, 2)
                comp["delta_percent"] = round(delta_pct, 2)

                if delta_pct > threshold_percent:
                    comp["status"] = "regression"
                    regressions.append(qid)
                elif delta_pct < -threshold_percent:
                    comp["status"] = "improvement"
                    improvements.append(qid)
                else:
                    comp["status"] = "stable"
            else:
                comp["status"] = "missing_data"

            comparisons.append(comp)

        # Calculate total runtime delta
        total1 = sum(timings1.values())
        total2 = sum(timings2.values())
        total_delta_pct = ((total2 - total1) / total1 * 100) if total1 > 0 else 0

        return {
            "baseline": {
                "file": file1,
                "platform": data1.get("platform", {}).get("type"),
                "benchmark": data1.get("benchmark"),
                "total_runtime_ms": round(total1, 2),
            },
            "comparison": {
                "file": file2,
                "platform": data2.get("platform", {}).get("type"),
                "benchmark": data2.get("benchmark"),
                "total_runtime_ms": round(total2, 2),
            },
            "summary": {
                "total_delta_percent": round(total_delta_pct, 2),
                "regressions": len(regressions),
                "improvements": len(improvements),
                "stable": len(comparisons) - len(regressions) - len(improvements),
                "threshold_percent": threshold_percent,
            },
            "regressions": regressions,
            "improvements": improvements,
            "query_comparisons": comparisons,
        }

    @mcp.tool(annotations=RESULTS_READONLY_ANNOTATIONS)
    def export_summary(
        result_file: str,
        format: str = "text",
    ) -> dict[str, Any]:
        """Export a formatted summary of benchmark results.

        Args:
            result_file: Name of the result file
            format: Output format ('text', 'markdown', or 'json')

        Returns:
            Formatted summary of the benchmark results.

        Example:
            export_summary(result_file="run.json", format="markdown")
        """
        results = _get_results_impl(result_file, include_queries=True)

        if "error" in results:
            return results

        if format == "json":
            return results

        # Build text/markdown summary
        lines = []

        if format == "markdown":
            lines.append(f"# Benchmark Results: {results.get('benchmark', 'Unknown')}")
            lines.append("")
            lines.append(f"**Platform**: {results.get('platform', {}).get('type', 'Unknown')}")
            lines.append(f"**Scale Factor**: {results.get('scale_factor', 'Unknown')}")
            lines.append(f"**Execution ID**: {results.get('execution_id', 'Unknown')}")
            lines.append("")

            if "summary" in results:
                summary = results["summary"]
                lines.append("## Summary")
                lines.append("")
                lines.append(f"- Total Queries: {summary.get('total_queries', 'N/A')}")
                lines.append(f"- Total Runtime: {summary.get('total_runtime_ms', 'N/A')} ms")
                lines.append("")

            if "queries" in results:
                lines.append("## Query Results")
                lines.append("")
                lines.append("| Query | Runtime (ms) | Status |")
                lines.append("|-------|-------------|--------|")
                for q in results.get("queries", [])[:20]:
                    lines.append(
                        f"| {q.get('query_id', 'N/A')} | {q.get('runtime_ms', 'N/A')} | {q.get('status', 'N/A')} |"
                    )
        else:
            # Plain text format
            lines.append(f"Benchmark Results: {results.get('benchmark', 'Unknown')}")
            lines.append(f"Platform: {results.get('platform', {}).get('type', 'Unknown')}")
            lines.append(f"Scale Factor: {results.get('scale_factor', 'Unknown')}")
            lines.append("")

            if "summary" in results:
                summary = results["summary"]
                lines.append("Summary:")
                lines.append(f"  Total Queries: {summary.get('total_queries', 'N/A')}")
                lines.append(f"  Total Runtime: {summary.get('total_runtime_ms', 'N/A')} ms")

        return {
            "format": format,
            "content": "\n".join(lines),
        }

    # Tool annotations for export (creates files)
    EXPORT_ANNOTATIONS = ToolAnnotations(
        title="Export benchmark results",
        readOnlyHint=False,  # Can create output files
        destructiveHint=False,
        idempotentHint=True,
        openWorldHint=False,
    )

    @mcp.tool(annotations=EXPORT_ANNOTATIONS)
    def export_results(
        result_file: str,
        format: str = "json",
        output_path: str | None = None,
    ) -> dict[str, Any]:
        """Export benchmark results to different formats.

        Exports results to JSON, CSV, or HTML format. If output_path is provided,
        writes to that file; otherwise returns the formatted content.

        Args:
            result_file: Name of the result file to export
            format: Output format ('json', 'csv', or 'html')
            output_path: Optional file path to write output (relative to results dir)

        Returns:
            Export status with content or file path.

        Example:
            export_results(result_file="run.json", format="csv")
            export_results(result_file="run.json", format="html", output_path="report.html")
        """
        import csv
        import html
        import io

        # Validate format
        valid_formats = ["json", "csv", "html"]
        if format.lower() not in valid_formats:
            return make_error(
                ErrorCode.VALIDATION_INVALID_FORMAT,
                f"Invalid format: {format}",
                details={"valid_formats": valid_formats},
                suggestion=f"Use one of: {', '.join(valid_formats)}",
            )

        # Get the results data
        results = _get_results_impl(result_file, include_queries=True)

        if "error" in results:
            return results

        format_lower = format.lower()
        content: str = ""

        if format_lower == "json":
            content = json.dumps(results, indent=2, default=str)

        elif format_lower == "csv":
            # Export query-level results as CSV
            output = io.StringIO()
            queries = results.get("queries", [])

            if queries:
                fieldnames = ["query_id", "phase", "runtime_ms", "status"]
                writer = csv.DictWriter(output, fieldnames=fieldnames)
                writer.writeheader()
                for q in queries:
                    writer.writerow(
                        {
                            "query_id": q.get("query_id", ""),
                            "phase": q.get("phase", ""),
                            "runtime_ms": q.get("runtime_ms", ""),
                            "status": q.get("status", ""),
                        }
                    )
                content = output.getvalue()
            else:
                content = "query_id,phase,runtime_ms,status\n"

        elif format_lower == "html":
            # Generate HTML report with XSS prevention via html.escape()
            esc = html.escape  # Shorthand for readability
            platform_type = esc(str(results.get("platform", {}).get("type", "Unknown")))
            benchmark_name = esc(str(results.get("benchmark", "Unknown")))
            scale_factor = esc(str(results.get("scale_factor", "Unknown")))
            execution_id = esc(str(results.get("execution_id", "Unknown")))

            html_parts = [
                "<!DOCTYPE html>",
                "<html><head>",
                f"<title>Benchmark Results: {benchmark_name}</title>",
                "<style>",
                "body { font-family: -apple-system, BlinkMacSystemFont, sans-serif; margin: 40px; }",
                "h1 { color: #333; }",
                "table { border-collapse: collapse; width: 100%; margin-top: 20px; }",
                "th, td { border: 1px solid #ddd; padding: 8px; text-align: left; }",
                "th { background-color: #4CAF50; color: white; }",
                "tr:nth-child(even) { background-color: #f2f2f2; }",
                ".summary { background: #f8f9fa; padding: 15px; border-radius: 5px; margin: 20px 0; }",
                "</style>",
                "</head><body>",
                f"<h1>Benchmark Results: {benchmark_name}</h1>",
                "<div class='summary'>",
                f"<p><strong>Platform:</strong> {platform_type}</p>",
                f"<p><strong>Scale Factor:</strong> {scale_factor}</p>",
                f"<p><strong>Execution ID:</strong> {execution_id}</p>",
            ]

            if "summary" in results:
                summary = results["summary"]
                total_queries = esc(str(summary.get("total_queries", "N/A")))
                total_runtime = esc(str(summary.get("total_runtime_ms", "N/A")))
                html_parts.append(f"<p><strong>Total Queries:</strong> {total_queries}</p>")
                html_parts.append(f"<p><strong>Total Runtime:</strong> {total_runtime} ms</p>")

            html_parts.append("</div>")

            # Query results table
            queries = results.get("queries", [])
            if queries:
                html_parts.extend(
                    [
                        "<h2>Query Results</h2>",
                        "<table>",
                        "<tr><th>Query</th><th>Phase</th><th>Runtime (ms)</th><th>Status</th></tr>",
                    ]
                )
                for q in queries:
                    q_id = esc(str(q.get("query_id", "")))
                    q_phase = esc(str(q.get("phase", "")))
                    q_runtime = esc(str(q.get("runtime_ms", "")))
                    q_status = esc(str(q.get("status", "")))
                    html_parts.append(
                        f"<tr><td>{q_id}</td><td>{q_phase}</td><td>{q_runtime}</td><td>{q_status}</td></tr>"
                    )
                html_parts.append("</table>")

            html_parts.extend(["</body></html>"])
            content = "\n".join(html_parts)

        # Write to file if output_path provided
        if output_path:
            # Validate output path (prevent path traversal)
            # First: simple string checks
            if ".." in output_path or output_path.startswith("/"):
                return make_error(
                    ErrorCode.VALIDATION_ERROR,
                    "Invalid output path",
                    details={"path": output_path},
                    suggestion="Use a relative path without '..' components",
                )

            # Second: resolve paths and verify containment (defense in depth)
            output_file = DEFAULT_RESULTS_DIR / output_path
            try:
                results_dir_resolved = DEFAULT_RESULTS_DIR.resolve()
                output_file_resolved = output_file.resolve()
                # Ensure the output path is within the results directory
                if not str(output_file_resolved).startswith(str(results_dir_resolved)):
                    return make_error(
                        ErrorCode.VALIDATION_ERROR,
                        "Output path escapes allowed directory",
                        details={"path": output_path},
                        suggestion="Use a path within the results directory",
                    )
            except Exception:
                pass  # Path resolution failed, let the write fail naturally

            try:
                output_file.parent.mkdir(parents=True, exist_ok=True)
                output_file.write_text(content)
                return {
                    "status": "exported",
                    "format": format_lower,
                    "source_file": result_file,
                    "output_path": str(output_file),
                    "size_bytes": len(content),
                }
            except Exception as e:
                return make_error(
                    ErrorCode.INTERNAL_ERROR,
                    f"Failed to write output file: {e}",
                    details={"output_path": output_path, "exception_type": type(e).__name__},
                )

        # Return content directly
        return {
            "status": "exported",
            "format": format_lower,
            "source_file": result_file,
            "content": content if len(content) < 50000 else content[:50000] + "\n... (truncated)",
            "size_bytes": len(content),
            "truncated": len(content) >= 50000,
        }
