"""
Core lifecycle runner package.

Exports the programmatic lifecycle API for running benchmarks without the CLI.
"""

from .runner import LifecyclePhases, ValidationOptions, run_benchmark_lifecycle

__all__ = ["run_benchmark_lifecycle", "LifecyclePhases", "ValidationOptions"]
