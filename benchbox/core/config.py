"""Core data structures for benchmark configuration, results, and execution.

This module re-exports configuration models from schemas.py for backward compatibility.
All models now use Pydantic for validation.

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

# Re-export all schema models
from benchbox.core.schemas import (
    BenchmarkConfig,
    DatabaseConfig,
    DryRunResult,
    LibraryInfo,
    PlatformInfo,
    QueryResult,
    RunConfig,
    SystemProfile,
)

__all__ = [
    "QueryResult",
    "RunConfig",
    "BenchmarkConfig",
    "DatabaseConfig",
    "SystemProfile",
    "LibraryInfo",
    "PlatformInfo",
    "DryRunResult",
]
