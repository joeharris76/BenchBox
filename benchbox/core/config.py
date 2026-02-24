"""Compatibility exports for ``benchbox.core.config`` imports.

The canonical models live in :mod:`benchbox.core.schemas`. This module
re-exports them so that existing examples and external code that import
from ``benchbox.core.config`` continue to work.

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

from benchbox.core.schemas import BenchmarkConfig, DatabaseConfig, RunConfig

__all__ = ["BenchmarkConfig", "DatabaseConfig", "RunConfig"]
