"""Plot benchmark performance trends command."""

from __future__ import annotations

import sys
from pathlib import Path

import click

from benchbox.cli.shared import console


@click.command("plot")
@click.argument("input_file", type=click.Path(exists=True))
@click.option(
    "--output",
    "output_file",
    type=click.Path(),
    required=True,
    help="Output image file path (PNG, SVG, or PDF)",
)
@click.option(
    "--metric",
    type=click.Choice(["geometric_mean", "p50", "p95", "p99", "total_time"], case_sensitive=False),
    default="geometric_mean",
    help="Metric to plot (default: geometric_mean)",
)
@click.option(
    "--title",
    type=str,
    help="Chart title (auto-generated if not provided)",
)
@click.option(
    "--width",
    type=int,
    default=12,
    help="Figure width in inches (default: 12)",
)
@click.option(
    "--height",
    type=int,
    default=6,
    help="Figure height in inches (default: 6)",
)
@click.pass_context
def plot(ctx, input_file, output_file, metric, title, width, height):
    """Visualize benchmark performance trends from CSV data.

    Generate line charts showing performance trends over time from aggregated
    benchmark results. Supports multiple metrics and customizable output formats.

    Requires matplotlib to be installed:
        pip install matplotlib

    Examples:
        # Plot geometric mean trends
        benchbox plot trends.csv --output chart.png

        # Plot P95 latency
        benchbox plot trends.csv --output p95.png --metric p95

        # Custom title and size
        benchbox plot trends.csv --output chart.svg \\
          --title "TPC-H Performance Trend" \\
          --width 14 --height 8

        # Export as PDF
        benchbox plot trends.csv --output report.pdf --metric total_time
    """
    input_path = Path(input_file)
    output_path = Path(output_file)

    # Check if matplotlib is available
    try:
        import matplotlib.pyplot as plt
        import pandas as pd
    except ImportError:
        console.print("[red]Error: matplotlib and pandas are required for plotting[/red]")
        console.print("\nInstall with:")
        console.print("  uv add matplotlib pandas")
        console.print("\nAlternative:")
        console.print("  uv pip install matplotlib pandas")
        console.print("  pip install matplotlib pandas")
        sys.exit(1)

    # Load CSV data
    try:
        df = pd.read_csv(input_path)
    except Exception as e:
        console.print(f"[red]Error loading CSV file: {e}[/red]")
        sys.exit(1)

    if df.empty:
        console.print("[yellow]No data found in CSV file[/yellow]")
        sys.exit(1)

    # Validate metric column exists
    metric_col_map = {
        "geometric_mean": "geometric_mean_ms",
        "p50": "p50_ms",
        "p95": "p95_ms",
        "p99": "p99_ms",
        "total_time": "total_time_s",
    }

    metric_col = metric_col_map.get(metric.lower())
    if metric_col not in df.columns:
        console.print(f"[red]Error: Column '{metric_col}' not found in CSV[/red]")
        console.print(f"Available columns: {', '.join(df.columns)}")
        sys.exit(1)

    # Generate plot
    try:
        fig, ax = plt.subplots(figsize=(width, height))

        # Group by benchmark/platform for multiple lines
        if "benchmark" in df.columns and "platform" in df.columns:
            for (benchmark, platform), group in df.groupby(["benchmark", "platform"]):  # type: ignore[not-iterable]
                label = f"{benchmark} ({platform})"
                ax.plot(group.index, group[metric_col], marker="o", label=label, linewidth=2)
        else:
            ax.plot(df.index, df[metric_col], marker="o", linewidth=2)

        # Formatting
        plot_title = title or f"Performance Trend - {metric.replace('_', ' ').title()}"
        ax.set_title(plot_title, fontsize=14, fontweight="bold")
        ax.set_xlabel("Run Index", fontsize=12)

        if metric == "total_time":
            ax.set_ylabel("Total Time (seconds)", fontsize=12)
        else:
            ax.set_ylabel("Time (milliseconds)", fontsize=12)

        ax.grid(True, alpha=0.3)
        ax.legend(loc="best")

        plt.tight_layout()

        # Save figure
        output_path.parent.mkdir(parents=True, exist_ok=True)
        plt.savefig(output_path, dpi=300, bbox_inches="tight")
        plt.close()

        console.print("[bold green]✓ Plot generated successfully![/bold green]")
        console.print(f"Output: {output_path.absolute()}")

        # Show file size
        size_kb = output_path.stat().st_size / 1024
        console.print(f"Size: {size_kb:.1f} KB")

    except Exception as e:
        console.print(f"[red]Error generating plot: {e}[/red]")
        import traceback

        console.print(traceback.format_exc())
        sys.exit(1)


__all__ = ["plot"]
