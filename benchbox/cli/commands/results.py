"""Results command implementation."""

import json
from pathlib import Path

import click
from pydantic import ValidationError
from rich.console import Console
from rich.panel import Panel

from benchbox.cli.output import ResultExporter
from benchbox.core.schemas import ExecutionContext

console = Console()


@click.group("results", invoke_without_command=True)
@click.option("--limit", type=int, default=10, help="Number of results to show")
@click.pass_context
def results(ctx, limit):
    """Show exported benchmark results and execution history.

    Displays a summary of recent benchmark executions including performance
    metrics, execution times, and result file locations.

    Examples:
        benchbox results              # Show last 10 results
        benchbox results --limit 25   # Show last 25 results
        benchbox results show-cli <file>  # Show CLI command to reproduce a run
    """
    # If no subcommand is invoked, show results summary
    if ctx.invoked_subcommand is None:
        exporter = ResultExporter()
        exporter.show_results_summary()


@results.command("show-cli")
@click.argument("result_file", type=click.Path(exists=True))
@click.option("--full", is_flag=True, help="Show full command with all options")
def show_cli(result_file: str, full: bool) -> None:
    """Reconstruct the CLI command from a benchmark result file.

    Reads a benchmark result JSON file and reconstructs the CLI command
    that can be used to reproduce the benchmark run.

    Examples:
        benchbox results show-cli benchmark_runs/results/tpch_sf001_duckdb_20240101_120000.json
        benchbox results show-cli ./result.json --full
    """
    try:
        result_path = Path(result_file)
        with result_path.open("r") as f:
            data = json.load(f)

        # Extract key identifiers
        platform = None
        benchmark_name = None
        scale_factor = None
        execution_context = None

        # Try schema v2.x format first
        if "benchmark" in data and isinstance(data["benchmark"], dict):
            benchmark_name = data["benchmark"].get("name") or data["benchmark"].get("id")
            scale_factor = data["benchmark"].get("scale_factor")

        if "platform" in data and isinstance(data["platform"], dict):
            platform = data["platform"].get("name")

        # Try legacy format
        if not platform:
            platform = data.get("platform")
        if not benchmark_name:
            benchmark_name = data.get("benchmark_name") or data.get("benchmark_id")
        if not scale_factor:
            scale_factor = data.get("scale_factor")

        # Get execution context
        execution_context = data.get("execution") or data.get("execution_context")

        if not all([platform, benchmark_name, scale_factor is not None]):
            console.print(
                "[red]❌ Could not extract required fields (platform, benchmark, scale_factor) from result file[/red]"
            )
            return

        # Build base command
        base_cmd = f"benchbox run --platform {platform} --benchmark {benchmark_name} --scale {scale_factor}"

        # If we have execution context, reconstruct full command
        if execution_context:
            ctx = ExecutionContext(**{k: v for k, v in execution_context.items() if k != "invocation_timestamp"})
            cli_args = ctx.to_cli_args()
            if cli_args:
                full_cmd = f"{base_cmd} {' '.join(cli_args)}"
            else:
                full_cmd = base_cmd
        else:
            full_cmd = base_cmd
            # Try to reconstruct from other fields
            extra_args = []

            # Query subset from run block
            if "run" in data and data["run"].get("query_subset"):
                extra_args.extend(["--queries", ",".join(data["run"]["query_subset"])])

            # Seed
            if execution_context and execution_context.get("seed"):
                extra_args.extend(["--seed", str(execution_context["seed"])])

            if extra_args:
                full_cmd = f"{base_cmd} {' '.join(extra_args)}"

        # Display the command
        console.print("\n[bold]Reconstructed CLI Command:[/bold]")
        console.print(Panel(full_cmd, style="green"))

        # Show provenance info
        if execution_context:
            entry_point = execution_context.get("entry_point", "unknown")
            timestamp = execution_context.get("timestamp") or execution_context.get("invocation_timestamp")
            console.print(f"\n[dim]Entry point: {entry_point}[/dim]")
            if timestamp:
                console.print(f"[dim]Executed: {timestamp}[/dim]")

        # If --full flag, show what options were non-default
        if full and execution_context:
            console.print("\n[bold]Execution Context Details:[/bold]")
            for key, value in execution_context.items():
                if key not in ("invocation_timestamp", "entry_point") and value is not None:
                    # Skip default values
                    if key == "phases" and value == ["power"]:
                        continue
                    if key == "mode" and value == "sql":
                        continue
                    if key == "compression_type" and value == "none":
                        continue
                    if key == "compression_enabled" and not value:
                        continue
                    if isinstance(value, bool) and not value:
                        continue
                    console.print(f"  {key}: {value}")

    except json.JSONDecodeError as e:
        console.print(f"[red]❌ Invalid JSON file: {e}[/red]")
    except (FileNotFoundError, PermissionError, IsADirectoryError) as e:
        console.print(f"[red]❌ Cannot read file: {e}[/red]")
    except (KeyError, TypeError) as e:
        console.print(f"[red]❌ Invalid result file format: {e}[/red]")
    except ValidationError as e:
        console.print(f"[red]❌ Invalid execution context in result file: {e}[/red]")


__all__ = ["results"]
