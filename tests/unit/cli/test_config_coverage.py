"""Coverage additions for cli/config.py."""

from __future__ import annotations

import importlib
from pathlib import Path
from unittest.mock import patch

import pytest

cfg = importlib.import_module("benchbox.cli.config")

pytestmark = [
    pytest.mark.unit,
    pytest.mark.fast,
]


def test_environment_overrides_apply_and_warn(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("BENCHBOX_DATABASE_PREFERRED", "duckdb")
    monkeypatch.setenv("BENCHBOX_SCALE_FACTOR", "0.5")
    monkeypatch.setenv("BENCHBOX_VERBOSE", "yes")
    monkeypatch.setenv("BENCHBOX_MAX_WORKERS", "bad")

    calls = []
    monkeypatch.setattr(cfg.console, "print", lambda *a, **k: calls.append(a[0] if a else ""))

    out = cfg._apply_environment_overrides({})
    assert out["database"]["preferred"] == "duckdb"
    assert out["benchmarks"]["default_scale"] == 0.5
    assert out["execution"]["verbose"] is True
    assert any("Invalid environment variable" in str(c) for c in calls)


def test_load_config_without_validation(monkeypatch: pytest.MonkeyPatch) -> None:
    manager = cfg.ConfigManager(config_path=Path("/tmp/nonexistent-benchbox-config.yaml"))
    monkeypatch.setattr(cfg, "ConfigManager", lambda config_path=None: manager)
    loaded = cfg.load_config(cli_args={"database": {"preferred": "sqlite"}}, validate=False)
    assert loaded.database["preferred"] == "sqlite"


def test_directory_manager_paths_and_cleanup(tmp_path: Path) -> None:
    dm = cfg.DirectoryManager(base_dir=str(tmp_path / "runs"))
    assert dm.results_dir.exists() and dm.datagen_dir.exists() and dm.databases_dir.exists()

    out = dm.get_result_path("tpch", 1.0, "duckdb", "2026-02-09T10:00:00", "abc")
    assert out.parent == dm.results_dir
    db = dm.get_database_path("tpch", 1.0, "duckdb")
    assert db.parent == dm.databases_dir
    dg = dm.get_datagen_path("tpch", 1.0)
    assert "tpch_sf1" in str(dg)

    old_file = dm.results_dir / "old.json"
    old_file.write_text("{}", encoding="utf-8")
    # Set the file's mtime to a very old timestamp so it's always older than cutoff
    import os as _os

    _os.utime(old_file, (0, 0))
    cleaned = dm.clean_old_files(max_age_days=0)
    assert str(old_file) in cleaned

    listing = dm.list_files()
    assert "results" in listing and "databases" in listing and "datagen" in listing
    sizes = dm.get_directory_sizes()
    assert "total" in sizes


def test_tuning_config_parse_save_and_validate(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    mgr = cfg.ConfigManager(config_path=tmp_path / "none.yaml")

    tuning_path = tmp_path / "tuning.yaml"
    tuning_path.write_text(
        """
tpch:
  orders:
    partitioning:
      - order_date
    sorting:
      - name: orderkey
        type: INTEGER
        order: 1
""",
        encoding="utf-8",
    )
    tunings = mgr.load_tuning_config(tuning_path)
    assert "tpch" in tunings

    out_path = tmp_path / "saved.yaml"
    mgr.save_tuning_config(tunings, out_path, format="yaml")
    assert out_path.exists()

    # cover parse validation failure path
    with pytest.raises(ValueError):
        mgr._parse_table_tuning("orders", {"partitioning": "not-a-list"})

    # unified tuning save/load
    u = cfg.UnifiedTuningConfiguration()
    u.primary_keys.enabled = True
    u.foreign_keys.enabled = True
    unified = tmp_path / "unified.yaml"
    mgr.save_unified_tuning_config(u, unified, format="yaml")
    loaded = mgr.load_unified_tuning_config(unified, platform="duckdb")
    assert loaded.primary_keys.enabled is True


def test_example_argument_parser_helpers(monkeypatch: pytest.MonkeyPatch) -> None:
    parser = cfg.ExampleArgumentParser.create_benchmark_parser("x", default_scale=0.5)
    cfg.ExampleArgumentParser.add_tuning_arguments(parser)
    cfg.ExampleArgumentParser.add_execution_arguments(parser)
    cfg.ExampleArgumentParser.add_output_arguments(parser)
    cfg.ExampleArgumentParser.add_platform_arguments(parser, "duckdb")
    cfg.ExampleArgumentParser.add_platform_arguments(parser, "databricks")
    cfg.ExampleArgumentParser.add_platform_arguments(parser, "clickhouse")

    # valid parse
    with patch(
        "argparse.ArgumentParser.parse_args",
        return_value=type(
            "A",
            (),
            {"power_only": False, "throughput_only": False, "load_only": False, "verbose": True, "quiet": False},
        )(),
    ):
        args = cfg.ExampleArgumentParser.parse_and_validate_args(parser)
        assert args.verbose is True

    # mutual exclusion validation branch
    with patch(
        "argparse.ArgumentParser.parse_args",
        return_value=type(
            "A", (), {"power_only": True, "throughput_only": True, "load_only": False, "verbose": False, "quiet": False}
        )(),
    ):
        with pytest.raises(SystemExit):
            cfg.ExampleArgumentParser.parse_and_validate_args(parser)
