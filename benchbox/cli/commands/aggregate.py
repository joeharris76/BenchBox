"""Aggregate multiple benchmark results into trends."""

from __future__ import annotations

import csv
import json
import statistics
import sys
from pathlib import Path

import click

from benchbox.cli.shared import console


@click.command("aggregate")
@click.option(
    "--input-dir",
    type=click.Path(exists=True),
    required=True,
    help="Directory containing result JSON files",
)
@click.option(
    "--output-file",
    type=click.Path(),
    required=True,
    help="Output CSV file path for aggregated trends",
)
@click.option(
    "--benchmark",
    type=str,
    help="Filter by benchmark name",
)
@click.option(
    "--platform",
    type=str,
    help="Filter by platform name",
)
@click.pass_context
def aggregate(ctx, input_dir, output_file, benchmark, platform):
    """Aggregate multiple benchmark results into performance trends.

    Scan a directory for benchmark result files and aggregate timing metrics
    into a CSV file suitable for tracking performance over time or generating
    visualizations.

    Output includes: timestamp, benchmark, platform, scale, geometric mean,
    total time, and per-query statistics (p50, p95, p99).

    Examples:
        # Aggregate all results in directory
        benchbox aggregate --input-dir benchmark_runs/ --output-file trends.csv

        # Filter by benchmark
        benchbox aggregate \\
          --input-dir benchmark_runs/ \\
          --output-file tpch_trends.csv \\
          --benchmark tpch

        # Filter by platform
        benchbox aggregate \\
          --input-dir benchmark_runs/ \\
          --output-file duckdb_trends.csv \\
          --platform duckdb
    """
    input_path = Path(input_dir)
    output_path = Path(output_file)

    # Find all JSON result files
    result_files = list(input_path.glob("**/*.json"))

    if not result_files:
        console.print(f"[yellow]No JSON result files found in {input_dir}[/yellow]")
        sys.exit(1)

    console.print(f"[blue]Found {len(result_files)} result files[/blue]")

    # Load and aggregate results
    aggregated_data = []

    for result_file in result_files:
        try:
            with open(result_file, encoding="utf-8") as f:
                data = json.load(f)

            # Extract metadata from new schema
            execution = data.get("execution", {})
            benchmark_info = data.get("benchmark", {})
            configuration = data.get("configuration", {})
            results = data.get("results", {})

            bm_name = benchmark_info.get("name", "unknown")
            plat_name = execution.get("platform", "unknown")

            # Apply filters
            if benchmark and benchmark.lower() not in bm_name.lower():
                continue

            if platform and platform.lower() not in plat_name.lower():
                continue

            # Extract timing metrics
            timestamp = execution.get("timestamp", "")
            scale_factor = configuration.get("scale_factor", 0)
            duration_ms = execution.get("duration_ms", 0)
            total_time = duration_ms / 1000.0  # Convert to seconds

            # Extract query timings from new schema
            queries = results.get("queries", {})
            query_details = queries.get("details", [])

            if query_details:
                # Extract timing from query details
                times_ms = []
                for q in query_details:
                    if q.get("status") == "SUCCESS":
                        timing = q.get("timing", {})
                        exec_time_ms = timing.get("execution_ms", 0)
                        if exec_time_ms > 0:
                            times_ms.append(exec_time_ms)

                if times_ms:
                    geomean = statistics.geometric_mean(times_ms) if all(t > 0 for t in times_ms) else 0
                    p50 = statistics.median(times_ms)
                    p95 = statistics.quantiles(times_ms, n=20)[18] if len(times_ms) >= 20 else max(times_ms)
                    p99 = statistics.quantiles(times_ms, n=100)[98] if len(times_ms) >= 100 else max(times_ms)
                else:
                    geomean = p50 = p95 = p99 = 0
            else:
                geomean = p50 = p95 = p99 = 0

            aggregated_data.append(
                {
                    "timestamp": timestamp,
                    "benchmark": bm_name,
                    "platform": plat_name,
                    "scale_factor": scale_factor,
                    "total_time_s": total_time,
                    "geometric_mean_ms": geomean,
                    "p50_ms": p50,
                    "p95_ms": p95,
                    "p99_ms": p99,
                    "num_queries": len(query_details),
                    "file": result_file.name,
                }
            )

        except Exception as e:
            console.print(f"[yellow]Warning: Failed to process {result_file.name}: {e}[/yellow]")
            continue

    if not aggregated_data:
        console.print("[yellow]No results matched the filters[/yellow]")
        if benchmark:
            console.print(f"  Benchmark filter: {benchmark}")
        if platform:
            console.print(f"  Platform filter: {platform}")
        sys.exit(1)

    # Sort by timestamp
    aggregated_data.sort(key=lambda x: x["timestamp"])

    # Write to CSV
    try:
        output_path.parent.mkdir(parents=True, exist_ok=True)

        with open(output_path, "w", newline="", encoding="utf-8") as csvfile:
            fieldnames = [
                "timestamp",
                "benchmark",
                "platform",
                "scale_factor",
                "total_time_s",
                "geometric_mean_ms",
                "p50_ms",
                "p95_ms",
                "p99_ms",
                "num_queries",
                "file",
            ]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

            writer.writeheader()
            for row in aggregated_data:
                writer.writerow(row)

        console.print("\n[bold green]✓ Aggregation complete![/bold green]")
        console.print(f"Aggregated {len(aggregated_data)} results")
        console.print(f"Output: {output_path.absolute()}")

    except Exception as e:
        console.print(f"[red]Error writing output file: {e}[/red]")
        sys.exit(1)


__all__ = ["aggregate"]
