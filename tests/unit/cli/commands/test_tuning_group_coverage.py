"""Coverage tests for cli/commands/tuning_group.py."""

from __future__ import annotations

import importlib
from pathlib import Path
from types import SimpleNamespace

import pytest
from click.testing import CliRunner

mod = importlib.import_module("benchbox.cli.commands.tuning_group")

pytestmark = [pytest.mark.fast, pytest.mark.unit]


class _Cfg:
    def __init__(self):
        self.execution = SimpleNamespace(lazy_evaluation=False, engine_affinity=None, streaming_mode=False)
        self.parallelism = SimpleNamespace(worker_count=None, threads_per_worker=None, thread_count=4)
        self.memory = SimpleNamespace(chunk_size=None, spill_to_disk=False, memory_limit="8GB")
        self.gpu = SimpleNamespace(enabled=False, pool_type="none", spill_to_host=False, device_id=0)
        self.data_types = SimpleNamespace(dtype_backend="pyarrow", auto_categorize_strings=True)
        self.io = SimpleNamespace(memory_map=True)

    def get_enabled_settings(self):
        return [SimpleNamespace(value="parallelism.thread_count")]

    def get_summary(self):
        return {"setting_count": 1, "has_streaming": self.execution.streaming_mode, "has_gpu": self.gpu.enabled}


class _Mgr:
    def __init__(self):
        self.created = []

    def create_sample_unified_tuning_config(self, out, platform):
        self.created.append((str(out), platform))

    def load_unified_tuning_config(self, *_a, **_k):
        return {"ok": True}


def test_init_mode_validation_and_dispatch(monkeypatch: pytest.MonkeyPatch) -> None:
    mgr = _Mgr()
    r_bad = CliRunner().invoke(
        mod.tuning_group,
        ["init", "--platform", "duckdb", "--mode", "dataframe"],
        obj={"config": mgr},
    )
    assert r_bad.exit_code == 1

    monkeypatch.setattr(mod, "_init_sql_tuning", lambda *_a, **_k: None)
    r_sql = CliRunner().invoke(mod.tuning_group, ["init", "--platform", "duckdb", "--mode", "sql"], obj={"config": mgr})
    assert r_sql.exit_code == 0


def test_init_dataframe_and_profile_configs(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(mod, "save_dataframe_tuning", lambda *_a, **_k: None)
    monkeypatch.setattr(mod, "get_smart_defaults", lambda _p: _Cfg())
    monkeypatch.setattr(mod, "detect_system_profile", lambda: SimpleNamespace())
    monkeypatch.setattr(
        mod,
        "get_profile_summary",
        lambda _sp: {"cpu_cores": 4, "available_memory_gb": 16.0, "memory_category": "mid", "has_gpu": False},
    )

    res = CliRunner().invoke(
        mod.tuning_group, ["init", "--platform", "polars", "--mode", "dataframe", "--smart-defaults"]
    )
    assert res.exit_code == 0

    cfg = mod._create_profile_config("dask", "memory-constrained")
    assert cfg.memory.spill_to_disk is True


def test_validate_defaults_list_show_and_platforms(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    cfg_file = tmp_path / "t.yaml"
    cfg_file.write_text("x: 1\n", encoding="utf-8")

    monkeypatch.setattr(mod, "load_dataframe_tuning", lambda *_a, **_k: _Cfg())
    monkeypatch.setattr(mod, "validate_dataframe_tuning", lambda *_a, **_k: [])
    ok = CliRunner().invoke(mod.tuning_group, ["validate", str(cfg_file), "--platform", "polars"])
    assert ok.exit_code == 0

    monkeypatch.setattr(mod, "validate_dataframe_tuning", lambda *_a, **_k: ["warn"])
    monkeypatch.setattr(mod, "format_issues", lambda _i: "issues")
    monkeypatch.setattr(mod, "has_errors", lambda *_a, **_k: False)
    warn = CliRunner().invoke(mod.tuning_group, ["validate", str(cfg_file), "--platform", "polars"])
    assert warn.exit_code == 0

    monkeypatch.setattr(mod, "detect_system_profile", lambda: SimpleNamespace())
    monkeypatch.setattr(
        mod,
        "get_profile_summary",
        lambda _sp: {
            "cpu_cores": 8,
            "available_memory_gb": 32.0,
            "memory_category": "high",
            "has_gpu": True,
            "gpu_memory_gb": 8.0,
            "gpu_device_count": 1,
        },
    )
    monkeypatch.setattr(mod, "get_smart_defaults", lambda _p: _Cfg())
    d = CliRunner().invoke(mod.tuning_group, ["defaults", "--platform", "cudf"])
    assert d.exit_code == 0

    called = {}
    monkeypatch.setattr(mod, "display_tuning_list", lambda *_a, **_k: called.setdefault("list", True))
    monkeypatch.setattr(mod, "resolve_tuning", lambda **_k: SimpleNamespace(config_file=None))
    monkeypatch.setattr(mod, "display_tuning_show", lambda *_a, **_k: called.setdefault("show", True))
    mgr = _Mgr()
    l = CliRunner().invoke(mod.tuning_group, ["list", "--platform", "duckdb"], obj={"config": mgr})
    s = CliRunner().invoke(mod.tuning_group, ["show", "tuned", "--platform", "duckdb"], obj={"config": mgr})
    p = CliRunner().invoke(mod.tuning_group, ["platforms"], obj={"config": mgr})
    assert l.exit_code == 0 and s.exit_code == 0 and p.exit_code == 0
    assert called["list"] and called["show"]
