"""TPC-compliant official benchmark execution command."""

from __future__ import annotations

import sys

import click

from benchbox.cli.commands.run import run
from benchbox.cli.shared import console

# TPC-allowed scale factors according to specification
TPC_ALLOWED_SCALE_FACTORS = {
    1,
    10,
    30,
    100,
    300,
    1000,
    3000,
    10000,
    30000,
    100000,
}


@click.command("run-official", hidden=True, deprecated=True)
@click.argument("benchmark", type=click.Choice(["tpch", "tpcds"], case_sensitive=False))
@click.option(
    "--platform",
    type=str,
    required=True,
    help="Platform to run on (e.g., snowflake, databricks, bigquery)",
)
@click.option(
    "--scale",
    type=float,
    required=True,
    help="Scale factor (must be TPC-allowed: 1, 10, 30, 100, 300, 1000, 3000, 10000, 30000, 100000)",
)
@click.option(
    "--phases",
    type=str,
    required=True,
    help="Test phases to run (e.g., power, throughput, maintenance, or power,throughput)",
)
@click.option(
    "--streams",
    type=int,
    help="Number of concurrent streams for throughput test (required for throughput phase)",
)
@click.option(
    "--seed",
    type=int,
    help="Random seed for reproducible execution (required for official runs)",
)
@click.option(
    "--output",
    "output_dir",
    type=click.Path(),
    help="Output directory for results",
)
@click.option(
    "--verbose",
    "-v",
    is_flag=True,
    help="Enable verbose logging",
)
@click.option(
    "--validate-results",
    is_flag=True,
    help="Enable result validation for compliance",
)
@click.pass_context
def run_official(ctx, benchmark, platform, scale, phases, streams, seed, output_dir, verbose, validate_results):
    """Run TPC-compliant official benchmark tests.

    DEPRECATED: Use 'benchbox run --official' instead.

    Migration: Replace 'benchbox run-official tpch --platform X --scale Y'
    with 'benchbox run --platform X --benchmark tpch --scale Y --official'

    Execute benchmarks according to TPC specifications with proper validation,
    seed management, and result reporting suitable for official TPC submissions.

    TPC-H Tests:
    - Power Test: Sequential execution of all 22 queries in randomized order
    - Throughput Test: Concurrent execution with multiple query streams
    - Maintenance Test: Refresh functions (RF1, RF2) for data updates

    TPC-DS Tests:
    - Power Test: Sequential execution of all 99 queries in randomized order
    - Throughput Test: Concurrent execution with multiple query streams
    - Maintenance Test: LM (Load Maintenance) and DM (Data Maintenance)

    Examples:
        # TPC-H Power Test at scale 100
        benchbox run-official tpch --platform snowflake --scale 100 \\
          --phases power --seed 42 --output results/power/

        # TPC-H Throughput Test with 4 streams
        benchbox run-official tpch --platform snowflake --scale 100 \\
          --phases throughput --streams 4 --seed 42 --output results/throughput/

        # TPC-DS Power + Throughput Tests
        benchbox run-official tpcds --platform databricks --scale 1000 \\
          --phases power,throughput --streams 8 --seed 7 --output results/

        # With result validation
        benchbox run-official tpch --platform bigquery --scale 30 \\
          --phases power --seed 5 --validate-results
    """
    # Show deprecation warning
    console.print(
        "[yellow]DeprecationWarning: 'benchbox run-official' is deprecated. "
        "Use 'benchbox run --official' instead.[/yellow]"
    )
    console.print()

    # Validate scale factor
    if scale not in TPC_ALLOWED_SCALE_FACTORS:
        console.print(f"[red]Error: Scale factor {scale} is not TPC-compliant[/red]")
        console.print(f"Allowed scale factors: {sorted(TPC_ALLOWED_SCALE_FACTORS)}")
        sys.exit(1)

    # Validate seed requirement
    if seed is None:
        console.print("[yellow]Warning: No seed specified. Official TPC runs require a random seed.[/yellow]")
        console.print("Use --seed <N> for reproducible results")

    # Validate streams for throughput test
    phase_list = [p.strip().lower() for p in phases.split(",")]
    if "throughput" in phase_list and streams is None:
        console.print("[red]Error: --streams is required for throughput test[/red]")
        sys.exit(1)

    # Show compliance information
    console.print("[bold blue]TPC-Compliant Official Benchmark Run[/bold blue]")
    console.print(f"Benchmark: TPC-{benchmark.upper()}")
    console.print(f"Platform: {platform}")
    console.print(f"Scale Factor: {scale}")
    console.print(f"Phases: {phases}")
    if streams:
        console.print(f"Streams: {streams}")
    if seed:
        console.print(f"Seed: {seed}")
    console.print("")

    # Build arguments for the run command
    run_args = {
        "platform": platform,
        "benchmark": benchmark,
        "scale": scale,
        "phases": phases,
        "official": True,  # Enable official mode in the new run command
    }

    if seed is not None:
        run_args["seed"] = seed

    if output_dir:
        run_args["output"] = output_dir

    if verbose:
        run_args["verbose"] = True

    if validate_results:
        run_args["validate_results"] = True

    # Note: streams parameter may need to be passed as environment variable or config
    # This would need to be implemented in the run command
    if streams:
        console.print(
            f"[yellow]Note: Stream configuration ({streams} streams) will be applied "
            "if supported by the benchmark[/yellow]"
        )
        # TODO: Pass streams to run command when multi-stream support is implemented
        # run_args["streams"] = streams

    # Invoke the run command
    try:
        ctx.invoke(run, **run_args)
    except Exception as e:
        console.print(f"[red]Benchmark execution failed: {e}[/red]")
        sys.exit(1)


__all__ = ["run_official"]
