"""Coverage tests for cli/commands/tuning.py."""

from __future__ import annotations

import importlib
from pathlib import Path
from types import SimpleNamespace

import pytest
from click.testing import CliRunner

mod = importlib.import_module("benchbox.cli.commands.tuning")

pytestmark = [pytest.mark.fast, pytest.mark.unit]


class _Mgr:
    def create_sample_unified_tuning_config(self, out, platform):
        Path(out).write_text(f"{platform}\n", encoding="utf-8")


def test_create_sample_tuning_success_and_failure(tmp_path: Path) -> None:
    out = tmp_path / "cfg.yaml"
    ok = CliRunner().invoke(
        mod.create_sample_tuning,
        ["--platform", "duckdb", "--output", str(out)],
        obj={"config": _Mgr()},
    )
    assert ok.exit_code == 0
    assert out.exists()

    bad_mgr = SimpleNamespace(
        create_sample_unified_tuning_config=lambda *_a, **_k: (_ for _ in ()).throw(RuntimeError("x"))
    )
    bad = CliRunner().invoke(mod.create_sample_tuning, ["--platform", "duckdb"], obj={"config": bad_mgr})
    assert bad.exit_code == 1
