"""
Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.

CLI utilities for BenchBox.
"""

# Import submodules to ensure they're accessible as attributes
# This is required for unittest.mock.patch() to work correctly in Python 3.10
from benchbox.cli import benchmarks  # noqa: F401
from benchbox.cli.main import main

__all__ = ["main"]
