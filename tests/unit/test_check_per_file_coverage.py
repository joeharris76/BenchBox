"""Tests for scripts/check_per_file_coverage.py."""

from __future__ import annotations

import json
import sys
from importlib import util
from pathlib import Path

import pytest

pytestmark = pytest.mark.fast


def _load_module() -> object:
    module_path = Path(__file__).resolve().parents[2] / "scripts" / "check_per_file_coverage.py"
    spec = util.spec_from_file_location("check_per_file_coverage", module_path)
    assert spec is not None
    assert spec.loader is not None
    module = util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _write_coverage_json(path: Path, files: dict[str, tuple[float, int]]) -> None:
    payload = {
        "files": {
            file_path: {
                "summary": {
                    "percent_covered": percent,
                    "num_statements": statements,
                }
            }
            for file_path, (percent, statements) in files.items()
        }
    }
    path.write_text(json.dumps(payload), encoding="utf-8")


def test_missing_coverage_is_non_fatal_by_default(capsys: pytest.CaptureFixture[str], tmp_path: Path) -> None:
    module = _load_module()
    args = module.parse_args(["--coverage-json", str(tmp_path / "missing.json")])
    code = module.run(args)
    captured = capsys.readouterr()
    assert code == 0
    assert "coverage file not found" in captured.out


def test_missing_coverage_can_fail(capsys: pytest.CaptureFixture[str], tmp_path: Path) -> None:
    module = _load_module()
    args = module.parse_args(["--coverage-json", str(tmp_path / "missing.json"), "--fail-on-missing-coverage"])
    code = module.run(args)
    captured = capsys.readouterr()
    assert code == 1
    assert "coverage file not found" in captured.out


def test_deferred_files_are_excluded_from_active_scope(capsys: pytest.CaptureFixture[str], tmp_path: Path) -> None:
    module = _load_module()
    coverage_path = tmp_path / "coverage.json"
    pyproject_path = tmp_path / "pyproject.toml"
    _write_coverage_json(
        coverage_path,
        {
            "benchbox/core/a.py": (60.0, 10),
            "benchbox/core/deferred.py": (0.0, 100),
        },
    )
    pyproject_path.write_text(
        '[tool.benchbox.coverage]\ndeferred_files = ["benchbox/core/deferred.py"]\n',
        encoding="utf-8",
    )

    args = module.parse_args(
        [
            "--coverage-json",
            str(coverage_path),
            "--deferred-from-pyproject",
            str(pyproject_path),
            "--min",
            "50",
        ]
    )
    code = module.run(args)
    captured = capsys.readouterr()
    assert code == 0
    assert "No active-scope files below threshold." in captured.out


def test_fails_when_active_file_below_threshold(capsys: pytest.CaptureFixture[str], tmp_path: Path) -> None:
    module = _load_module()
    coverage_path = tmp_path / "coverage.json"
    _write_coverage_json(
        coverage_path,
        {
            "benchbox/core/good.py": (88.0, 20),
            "benchbox/core/bad.py": (12.5, 80),
        },
    )

    args = module.parse_args(["--coverage-json", str(coverage_path), "--min", "50"])
    code = module.run(args)
    captured = capsys.readouterr()
    assert code == 1
    assert "Files below threshold:" in captured.out
    assert "benchbox/core/bad.py" in captured.out


def test_report_only_does_not_fail(capsys: pytest.CaptureFixture[str], tmp_path: Path) -> None:
    module = _load_module()
    coverage_path = tmp_path / "coverage.json"
    _write_coverage_json(coverage_path, {"benchbox/core/bad.py": (5.0, 20)})

    args = module.parse_args(["--coverage-json", str(coverage_path), "--min", "50", "--report-only"])
    code = module.run(args)
    captured = capsys.readouterr()
    assert code == 0
    assert "benchbox/core/bad.py" in captured.out
