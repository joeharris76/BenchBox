"""Unit tests for benchbox.cli package exports.

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

from __future__ import annotations

import importlib
import sys

import pytest

pytestmark = [
    pytest.mark.unit,
    pytest.mark.fast,
]


def test_benchmarks_submodule_remains_available_as_package_attribute(monkeypatch: pytest.MonkeyPatch) -> None:
    """Accessing ``benchbox.cli.benchmarks`` should import the real submodule once."""
    monkeypatch.delitem(sys.modules, "benchbox.cli.benchmarks", raising=False)
    monkeypatch.delitem(sys.modules, "benchbox.cli", raising=False)

    cli_module = importlib.import_module("benchbox.cli")
    benchmarks_module = cli_module.benchmarks

    assert benchmarks_module is importlib.import_module("benchbox.cli.benchmarks")
    assert cli_module.benchmarks is benchmarks_module
