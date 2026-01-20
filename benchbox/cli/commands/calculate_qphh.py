"""Calculate TPC-H QphH composite metric command."""

from __future__ import annotations

import json
import math
import sys
from pathlib import Path

import click

from benchbox.cli.shared import console


@click.command("calculate-qphh", hidden=True, deprecated=True)
@click.option(
    "--power-results",
    type=click.Path(exists=True),
    required=True,
    help="Path to power test results JSON file",
)
@click.option(
    "--throughput-results",
    type=click.Path(exists=True),
    required=True,
    help="Path to throughput test results JSON file",
)
@click.option(
    "--scale-factor",
    type=float,
    help="Scale factor used (auto-detected from results if not provided)",
)
@click.option(
    "--format",
    "output_format",
    type=click.Choice(["text", "json"], case_sensitive=False),
    default="text",
    help="Output format (default: text)",
)
@click.option(
    "--output",
    "output_file",
    type=click.Path(),
    help="Save output to file",
)
@click.pass_context
def calculate_qphh(ctx, power_results, throughput_results, scale_factor, output_format, output_file):
    """Calculate TPC-H QphH@Size composite metric.

    DEPRECATED: Use 'benchbox metrics qphh' instead.

    Migration: Replace 'benchbox calculate-qphh --power-results X --throughput-results Y'
    with 'benchbox metrics qphh --power-results X --throughput-results Y'

    Calculate the official TPC-H QphH@Size (Queries per Hour) composite
    metric from power test and throughput test results according to TPC-H
    specification.

    Formula: QphH@Size = sqrt(Power@Size × Throughput@Size)
    Where:
        Power@Size = 3600 × SF / Power_Test_Time
        Throughput@Size = Num_Streams × 3600 × SF / Throughput_Test_Time

    Examples:
        # Calculate QphH from test results
        benchbox calculate-qphh \\
          --power-results results/power/results.json \\
          --throughput-results results/throughput/results.json

        # Specify scale factor explicitly
        benchbox calculate-qphh \\
          --power-results power.json \\
          --throughput-results throughput.json \\
          --scale-factor 100

        # Export to JSON
        benchbox calculate-qphh \\
          --power-results power.json \\
          --throughput-results throughput.json \\
          --format json --output qphh.json
    """
    # Show deprecation warning
    console.print(
        "[yellow]DeprecationWarning: 'benchbox calculate-qphh' is deprecated. "
        "Use 'benchbox metrics qphh' instead.[/yellow]"
    )
    console.print()

    power_path = Path(power_results)
    throughput_path = Path(throughput_results)

    # Load result files
    try:
        with open(power_path, encoding="utf-8") as f:
            power_data = json.load(f)

        with open(throughput_path, encoding="utf-8") as f:
            throughput_data = json.load(f)

    except FileNotFoundError as e:
        console.print(f"[red]Error: Result file not found: {e}[/red]")
        sys.exit(1)
    except json.JSONDecodeError as e:
        console.print(f"[red]Error: Invalid JSON in result file: {e}[/red]")
        sys.exit(1)
    except Exception as e:
        console.print(f"[red]Unexpected error loading files: {e}[/red]")
        sys.exit(1)

    # Extract scale factor
    if scale_factor is None:
        # Try to auto-detect from results
        sf_power = power_data.get("environment", {}).get("scale_factor")
        sf_throughput = throughput_data.get("environment", {}).get("scale_factor")

        if sf_power and sf_throughput:
            if sf_power != sf_throughput:
                console.print(f"[red]Error: Scale factor mismatch: power={sf_power}, throughput={sf_throughput}[/red]")
                sys.exit(1)
            scale_factor = sf_power
        else:
            console.print("[red]Error: Could not auto-detect scale factor. Please specify --scale-factor[/red]")
            sys.exit(1)

    # Extract timing information
    power_time = power_data.get("results", {}).get("total_execution_time")
    throughput_time = throughput_data.get("results", {}).get("total_execution_time")

    if not power_time or not throughput_time:
        console.print("[red]Error: Could not extract execution times from result files[/red]")
        console.print("Expected 'results.total_execution_time' in both files")
        sys.exit(1)

    # Extract number of streams for throughput test
    num_streams = throughput_data.get("environment", {}).get("num_streams", 1)

    # Calculate metrics according to TPC-H specification
    power_at_size = (3600.0 * scale_factor) / power_time
    throughput_at_size = (num_streams * 3600.0 * scale_factor) / throughput_time
    qphh_at_size = math.sqrt(power_at_size * throughput_at_size)

    # Prepare output
    result = {
        "benchmark": "TPC-H",
        "scale_factor": scale_factor,
        "num_streams": num_streams,
        "power_test_time": power_time,
        "throughput_test_time": throughput_time,
        "power_at_size": power_at_size,
        "throughput_at_size": throughput_at_size,
        "qphh_at_size": qphh_at_size,
    }

    # Format output
    if output_format == "text":
        output_text = _format_text_output(result)
        if output_file:
            Path(output_file).write_text(output_text, encoding="utf-8")
            console.print(f"[green]Results saved to {output_file}[/green]")
        else:
            console.print(output_text)
    elif output_format == "json":
        output_json = json.dumps(result, indent=2)
        if output_file:
            Path(output_file).write_text(output_json, encoding="utf-8")
            console.print(f"[green]Results saved to {output_file}[/green]")
        else:
            console.print(output_json)


def _format_text_output(result: dict) -> str:
    """Format QphH calculation as human-readable text."""
    lines = []

    lines.append("=" * 70)
    lines.append("TPC-H QphH@Size CALCULATION")
    lines.append("=" * 70)
    lines.append("")
    lines.append(f"Benchmark:        {result['benchmark']}")
    lines.append(f"Scale Factor:     {result['scale_factor']}")
    lines.append(f"Num Streams:      {result['num_streams']}")
    lines.append("")
    lines.append("-" * 70)
    lines.append("TEST EXECUTION TIMES")
    lines.append("-" * 70)
    lines.append(f"Power Test:       {result['power_test_time']:.3f} seconds")
    lines.append(f"Throughput Test:  {result['throughput_test_time']:.3f} seconds")
    lines.append("")
    lines.append("-" * 70)
    lines.append("TPC-H METRICS")
    lines.append("-" * 70)
    lines.append(f"Power@Size:       {result['power_at_size']:,.2f}")
    lines.append(f"Throughput@Size:  {result['throughput_at_size']:,.2f}")
    lines.append("")
    lines.append("=" * 70)
    lines.append(f"QphH@Size:        {result['qphh_at_size']:,.2f}")
    lines.append("=" * 70)
    lines.append("")
    lines.append("Formula: QphH@Size = sqrt(Power@Size × Throughput@Size)")
    lines.append("  Power@Size = 3600 × SF / Power_Test_Time")
    lines.append("  Throughput@Size = Num_Streams × 3600 × SF / Throughput_Test_Time")
    lines.append("")

    return "\n".join(lines)


__all__ = ["calculate_qphh"]
