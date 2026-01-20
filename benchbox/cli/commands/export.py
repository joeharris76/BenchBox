"""Result export command implementation."""

from __future__ import annotations

from pathlib import Path

import click

from benchbox.cli.output import ResultExporter
from benchbox.cli.shared import console
from benchbox.core.results.loader import (
    ResultLoadError,
    UnsupportedSchemaError,
    find_latest_result,
    load_result_file,
)


@click.command("export")
@click.argument("result_file", required=False, type=click.Path(exists=True))
@click.option(
    "--format",
    "formats",
    multiple=True,
    type=click.Choice(["json", "csv", "html"], case_sensitive=False),
    help="Export format(s) - can specify multiple (default: json)",
)
@click.option(
    "--output-dir",
    type=str,
    help="Output directory (default: benchmark_runs/results/)",
)
@click.option(
    "--last",
    is_flag=True,
    help="Export most recent result file",
)
@click.option(
    "--benchmark",
    type=str,
    help="Filter by benchmark name when using --last",
)
@click.option(
    "--platform",
    type=str,
    help="Filter by platform name when using --last",
)
@click.option(
    "--force",
    is_flag=True,
    help="Overwrite existing files without prompting",
)
@click.pass_context
def export(ctx, result_file, formats, output_dir, last, benchmark, platform, force):
    """Export benchmark results to various formats.

    Re-export existing benchmark results in different formats (JSON, CSV, HTML)
    without re-running the benchmark. Useful for sharing results, generating
    reports, or converting to spreadsheet-friendly formats.

    RESULT_FILE: Path to result JSON file to export (optional)

    Examples:
        # Export specific result to CSV
        benchbox export results/tpch_sf1_duckdb.json --format csv

        # Export to multiple formats
        benchbox export results/tpcds_sf10.json --format csv --format html

        # Export most recent result
        benchbox export --last --format html

        # Export latest TPC-H result to all formats
        benchbox export --last --benchmark tpc_h --format json --format csv --format html

        # Export to custom directory
        benchbox export --last --format csv --output-dir ./reports/
    """
    # Default to JSON if no formats specified
    if not formats:
        formats = ["json"]

    # Determine source file
    source_path = None

    if result_file:
        # Explicit file path provided
        source_path = Path(result_file)
    elif last:
        # Find latest result with optional filters
        default_results_dir = Path("benchmark_runs/results")
        source_path = find_latest_result(
            default_results_dir,
            benchmark=benchmark,
            platform=platform,
        )

        if not source_path:
            console.print("[yellow]No results found[/yellow]")
            if benchmark or platform:
                filters = []
                if benchmark:
                    filters.append(f"benchmark={benchmark}")
                if platform:
                    filters.append(f"platform={platform}")
                console.print(f"  Filters: {', '.join(filters)}")
            console.print("\n[dim]Tip: Run a benchmark first or check benchmark_runs/results/[/dim]")
            return

        console.print(f"[blue]Using latest result:[/blue] {source_path.name}")
    else:
        # No file specified, show usage help
        console.print("[yellow]Please specify a result file or use --last[/yellow]")
        console.print("\n[bold]Examples:[/bold]")
        console.print("  benchbox export result.json --format csv")
        console.print("  benchbox export --last --format html")
        console.print("  benchbox export --last --benchmark tpc_h --format csv")
        console.print("\n[dim]Tip: Use 'benchbox results' to see available result files[/dim]")
        return

    # Load result file
    try:
        result, raw_data = load_result_file(source_path)
        console.print(f"[green]✓[/green] Loaded: {result.benchmark_name} ({result.platform})")
        console.print(
            f"  Scale: {result.scale_factor}, Queries: {result.total_queries}, Duration: {result.duration_seconds:.2f}s"
        )
    except FileNotFoundError:
        console.print(f"[red]Error: Result file not found: {source_path}[/red]")
        return
    except UnsupportedSchemaError as e:
        console.print(f"[red]Error: {e}[/red]")
        console.print("[dim]Only schema version 1.0 is currently supported[/dim]")
        return
    except ResultLoadError as e:
        console.print(f"[red]Error loading result file: {e}[/red]")
        console.print("[dim]The file may be corrupted or in an invalid format[/dim]")
        return
    except Exception as e:
        console.print(f"[red]Unexpected error: {e}[/red]")
        return

    # Determine output directory
    if output_dir:
        output_directory = Path(output_dir)
    else:
        # Default to benchmark_runs/results/
        output_directory = Path("benchmark_runs/results")

    # Ensure output directory exists
    output_directory.mkdir(parents=True, exist_ok=True)

    # Export to requested formats
    try:
        exporter = ResultExporter(output_dir=output_directory)

        # Check for existing files if not --force
        if not force:
            base_name = source_path.stem
            conflicts = []
            for fmt in formats:
                potential_file = output_directory / f"{base_name}.{fmt}"
                if potential_file.exists() and potential_file != source_path:
                    conflicts.append(fmt)

            if conflicts:
                console.print("\n[yellow]Warning: The following files will be overwritten:[/yellow]")
                for fmt in conflicts:
                    console.print(f"  • {base_name}.{fmt}")

                if not click.confirm("\nProceed with export?", default=True):
                    console.print("[yellow]Export cancelled[/yellow]")
                    return

        # Perform export
        console.print(f"\n[bold]Exporting to {len(formats)} format(s)...[/bold]")
        exported = exporter.export_result(result, formats=list(formats))

        # Display success summary
        console.print("\n[bold green]✓ Export complete![/bold green]")
        console.print(f"Exported {len(exported)} format(s):")

        for fmt, path in exported.items():
            try:
                size_kb = path.stat().st_size / 1024
                console.print(f"  • {fmt.upper()}: {path.name} ({size_kb:.1f} KB)")
            except OSError:
                console.print(f"  • {fmt.upper()}: {path.name}")

        console.print(f"\n[dim]Output directory: {output_directory.absolute()}[/dim]")

    except Exception as e:
        console.print(f"\n[red]Export failed: {e}[/red]")
        return


__all__ = ["export"]
