"""Coverage tests for cli/commands/datagen.py."""

from __future__ import annotations

import importlib
from types import SimpleNamespace

import pytest
from click.testing import CliRunner

mod = importlib.import_module("benchbox.cli.commands.datagen")

pytestmark = [pytest.mark.fast, pytest.mark.unit]


def test_parse_run_args_types_and_flags() -> None:
    parsed = mod._parse_run_args(["--scale", "0.1", "--seed", "7", "--verbose", "--benchmark", "tpch"])
    assert parsed["scale"] == 0.1
    assert parsed["seed"] == 7
    assert parsed["verbose"] is True
    assert parsed["benchmark"] == "tpch"


def test_datagen_invokes_run_with_generate_phase(monkeypatch: pytest.MonkeyPatch) -> None:
    captured = {}
    monkeypatch.setattr(mod, "run", lambda **kwargs: captured.update(kwargs))
    result = CliRunner().invoke(
        mod.datagen,
        ["--benchmark", "tpch", "--scale", "0.01", "--output", "outdir", "--seed", "42", "--verbose"],
    )
    assert result.exit_code == 0

    assert captured["platform"] == "duckdb"
    assert captured["benchmark"] == "tpch"
    assert captured["phases"] == "generate"
    assert captured["seed"] == 42
    assert captured["verbose"] is True


def test_datagen_handles_non_parquet_note(monkeypatch: pytest.MonkeyPatch) -> None:
    calls = []
    monkeypatch.setattr(mod.console, "print", lambda *a, **k: calls.append(a[0] if a else ""))
    monkeypatch.setattr(mod, "run", lambda **_kwargs: None)
    res = CliRunner().invoke(mod.datagen, ["--benchmark", "tpch", "--scale", "0.01", "--format", "json"])
    assert res.exit_code == 0
    assert any("Format 'json' requested" in str(c) for c in calls)
