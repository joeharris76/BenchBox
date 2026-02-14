"""Coverage tests for cli/commands/run_official.py."""

from __future__ import annotations

import importlib
from types import SimpleNamespace

import pytest
from click.testing import CliRunner

mod = importlib.import_module("benchbox.cli.commands.run_official")

pytestmark = [pytest.mark.fast, pytest.mark.unit]


def test_run_official_rejects_invalid_scale() -> None:
    res = CliRunner().invoke(
        mod.run_official,
        ["tpch", "--platform", "duckdb", "--scale", "2", "--phases", "power"],
    )
    assert res.exit_code == 1
    assert "not TPC-compliant" in res.output


def test_run_official_requires_streams_for_throughput() -> None:
    res = CliRunner().invoke(
        mod.run_official,
        ["tpch", "--platform", "duckdb", "--scale", "1", "--phases", "throughput"],
    )
    assert res.exit_code == 1
    assert "--streams is required" in res.output


def test_run_official_invokes_run_with_args() -> None:
    captured = {}
    mod.run = lambda **kwargs: captured.update(kwargs)
    res = CliRunner().invoke(
        mod.run_official,
        [
            "tpch",
            "--platform",
            "duckdb",
            "--scale",
            "1",
            "--phases",
            "power,throughput",
            "--streams",
            "4",
            "--seed",
            "42",
            "--output",
            "out",
            "--verbose",
            "--validate-results",
        ],
    )
    assert res.exit_code == 0
    assert captured["official"] is True
    assert captured["platform"] == "duckdb"
    assert captured["benchmark"] == "tpch"
    assert captured["validate_results"] is True


def test_run_official_handles_invoke_exception() -> None:
    mod.run = lambda **_kwargs: (_ for _ in ()).throw(RuntimeError("x"))
    res = CliRunner().invoke(
        mod.run_official,
        ["tpch", "--platform", "duckdb", "--scale", "1", "--phases", "power", "--seed", "1"],
    )
    assert res.exit_code == 1
