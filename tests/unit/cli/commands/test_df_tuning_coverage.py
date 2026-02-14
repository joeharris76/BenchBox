"""Coverage tests for cli/commands/df_tuning.py."""

from __future__ import annotations

import importlib
from pathlib import Path
from types import SimpleNamespace

import pytest
from click.testing import CliRunner

mod = importlib.import_module("benchbox.cli.commands.df_tuning")

pytestmark = [pytest.mark.fast, pytest.mark.unit]


class _Cfg:
    def __init__(self):
        self.execution = SimpleNamespace(lazy_evaluation=False, engine_affinity=None, streaming_mode=False)
        self.parallelism = SimpleNamespace(worker_count=None, threads_per_worker=None, thread_count=None)
        self.memory = SimpleNamespace(chunk_size=None, spill_to_disk=False, memory_limit=None)
        self.gpu = SimpleNamespace(enabled=False, pool_type="none", spill_to_host=False, device_id=0)
        self.data_types = SimpleNamespace(dtype_backend="numpy_nullable", auto_categorize_strings=False)
        self.io = SimpleNamespace(memory_map=False)

    def get_enabled_settings(self):
        return [SimpleNamespace(value="execution.streaming_mode")]

    def get_summary(self):
        return {"setting_count": 1, "has_streaming": self.execution.streaming_mode, "has_gpu": self.gpu.enabled}


class _Issue:
    def __init__(self, level):
        self.level = level


def test_create_sample_profile_and_smart_defaults(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    saved = []
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(mod, "DataFrameTuningConfiguration", _Cfg)
    monkeypatch.setattr(mod, "save_dataframe_tuning", lambda cfg, p: saved.append((cfg, Path(p))))
    monkeypatch.setattr(mod, "get_smart_defaults", lambda _p: _Cfg())
    monkeypatch.setattr(mod, "detect_system_profile", lambda: SimpleNamespace())
    monkeypatch.setattr(
        mod,
        "get_profile_summary",
        lambda _sp: {"cpu_cores": 8, "available_memory_gb": 32.0, "memory_category": "high", "has_gpu": False},
    )

    r1 = CliRunner().invoke(mod.df_tuning_group, ["create-sample", "--platform", "polars", "--profile", "optimized"])
    assert r1.exit_code == 0
    assert "deprecated" in r1.output.lower()
    assert saved[-1][1].name == "polars_optimized.yaml"

    r2 = CliRunner().invoke(mod.df_tuning_group, ["create-sample", "--platform", "polars", "--smart-defaults"])
    assert r2.exit_code == 0


def test_create_sample_aborts_on_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(mod, "DataFrameTuningConfiguration", _Cfg)
    monkeypatch.setattr(
        mod, "save_dataframe_tuning", lambda *_a, **_k: (_ for _ in ()).throw(RuntimeError("save boom"))
    )
    result = CliRunner().invoke(mod.df_tuning_group, ["create-sample", "--platform", "polars"])
    assert result.exit_code != 0
    assert "Failed to create sample configuration" in result.output


def test_validate_success_warning_and_error_paths(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    cfg_file = tmp_path / "x.yaml"
    cfg_file.write_text("x: 1\n", encoding="utf-8")
    monkeypatch.setattr(mod, "load_dataframe_tuning", lambda *_a, **_k: _Cfg())
    monkeypatch.setattr(mod, "format_issues", lambda _issues: "issues")

    monkeypatch.setattr(mod, "validate_dataframe_tuning", lambda *_a, **_k: [])
    ok = CliRunner().invoke(mod.df_tuning_group, ["validate", str(cfg_file), "--platform", "polars"])
    assert ok.exit_code == 0

    monkeypatch.setattr(mod, "validate_dataframe_tuning", lambda *_a, **_k: [_Issue("warning")])
    monkeypatch.setattr(mod, "has_errors", lambda *_a, **_k: False)
    warn = CliRunner().invoke(mod.df_tuning_group, ["validate", str(cfg_file), "--platform", "polars"])
    assert warn.exit_code == 0

    monkeypatch.setattr(mod, "has_errors", lambda *_a, **_k: True)
    bad = CliRunner().invoke(mod.df_tuning_group, ["validate", str(cfg_file), "--platform", "polars"])
    assert bad.exit_code != 0


def test_show_defaults_and_list_platforms(monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = _Cfg()
    cfg.parallelism.thread_count = 4
    cfg.memory.memory_limit = "8GB"
    cfg.execution.streaming_mode = True

    monkeypatch.setattr(mod, "detect_system_profile", lambda: SimpleNamespace())
    monkeypatch.setattr(
        mod,
        "get_profile_summary",
        lambda _sp: {
            "cpu_cores": 8,
            "available_memory_gb": 32.0,
            "memory_category": "high",
            "has_gpu": True,
            "gpu_memory_gb": 12.0,
            "gpu_device_count": 1,
        },
    )
    monkeypatch.setattr(mod, "get_smart_defaults", lambda _p: cfg)

    show = CliRunner().invoke(mod.df_tuning_group, ["show-defaults", "--platform", "cudf"])
    assert show.exit_code == 0
    assert "Recommended Settings" in show.output

    lst = CliRunner().invoke(mod.df_tuning_group, ["list-platforms"])
    assert lst.exit_code == 0
    assert "DataFrame Platforms" in lst.output
