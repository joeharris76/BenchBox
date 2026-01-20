"""Results command implementation."""

import click

from benchbox.cli.output import ResultExporter


@click.command("results")
@click.option("--limit", type=int, default=10, help="Number of results to show")
@click.pass_context
def results(ctx, limit):
    """Show exported benchmark results and execution history.

    Displays a summary of recent benchmark executions including performance
    metrics, execution times, and result file locations.

    Examples:
        benchbox results              # Show last 10 results
        benchbox results --limit 25   # Show last 25 results
    """
    exporter = ResultExporter()
    exporter.show_results_summary()


__all__ = ["results"]
