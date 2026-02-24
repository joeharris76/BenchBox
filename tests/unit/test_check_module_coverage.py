"""Tests for scripts/check_module_coverage.py."""

from __future__ import annotations

import sys
from importlib import util
from pathlib import Path

import pytest

pytestmark = pytest.mark.fast


def _load_module() -> object:
    module_path = Path(__file__).resolve().parents[2] / "scripts" / "check_module_coverage.py"
    spec = util.spec_from_file_location("check_module_coverage", module_path)
    assert spec is not None
    assert spec.loader is not None
    module = util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _write_coverage_xml(path: Path) -> None:
    xml = """<?xml version="1.0" ?>
<coverage>
  <packages>
    <package name="benchbox.cli">
      <classes>
        <class filename="benchbox/cli/a.py">
          <lines>
            <line number="1" hits="1" />
            <line number="2" hits="0" />
          </lines>
        </class>
        <class filename="benchbox/cli/b.py">
          <lines>
            <line number="1" hits="1" />
            <line number="2" hits="1" />
          </lines>
        </class>
      </classes>
    </package>
    <package name="benchbox.core">
      <classes>
        <class filename="benchbox/core/c.py">
          <lines>
            <line number="1" hits="0" />
            <line number="2" hits="0" />
            <line number="3" hits="1" />
            <line number="4" hits="1" />
          </lines>
        </class>
      </classes>
    </package>
    <package name="external">
      <classes>
        <class filename="external/vendor.py">
          <lines>
            <line number="1" hits="1" />
          </lines>
        </class>
      </classes>
    </package>
  </packages>
</coverage>
"""
    path.write_text(xml, encoding="utf-8")


def test_missing_coverage_xml_is_non_fatal(capsys: pytest.CaptureFixture[str], tmp_path: Path) -> None:
    module = _load_module()
    args = module.parse_args(["--coverage-xml", str(tmp_path / "missing.xml")])
    code = module.run(args)
    captured = capsys.readouterr()
    assert code == 0
    assert "coverage XML not found" in captured.out


def test_reports_modules_and_fails_when_below_threshold(capsys: pytest.CaptureFixture[str], tmp_path: Path) -> None:
    module = _load_module()
    coverage_path = tmp_path / "coverage.xml"
    _write_coverage_xml(coverage_path)

    args = module.parse_args(["--coverage-xml", str(coverage_path), "--threshold", "80"])
    code = module.run(args)
    captured = capsys.readouterr()
    assert code == 1
    assert "benchbox/cli" in captured.out
    assert "benchbox/core" in captured.out
    assert "FAIL" in captured.out


def test_passes_when_all_modules_meet_threshold(capsys: pytest.CaptureFixture[str], tmp_path: Path) -> None:
    module = _load_module()
    coverage_path = tmp_path / "coverage.xml"
    _write_coverage_xml(coverage_path)

    args = module.parse_args(["--coverage-xml", str(coverage_path), "--threshold", "50"])
    code = module.run(args)
    captured = capsys.readouterr()
    assert code == 0
    assert "module coverage check passed" in captured.out


def test_warns_when_no_modules_match_root(capsys: pytest.CaptureFixture[str], tmp_path: Path) -> None:
    module = _load_module()
    coverage_path = tmp_path / "coverage.xml"
    _write_coverage_xml(coverage_path)

    args = module.parse_args(["--coverage-xml", str(coverage_path), "--source-root", "notbenchbox"])
    code = module.run(args)
    captured = capsys.readouterr()
    assert code == 1
    assert "no modules matched source root" in captured.out


def test_allow_empty_permits_no_module_match(capsys: pytest.CaptureFixture[str], tmp_path: Path) -> None:
    module = _load_module()
    coverage_path = tmp_path / "coverage.xml"
    _write_coverage_xml(coverage_path)

    args = module.parse_args(["--coverage-xml", str(coverage_path), "--source-root", "notbenchbox", "--allow-empty"])
    code = module.run(args)
    captured = capsys.readouterr()
    assert code == 0
    assert "no modules matched source root" in captured.out


def test_absolute_path_is_mapped_to_module(capsys: pytest.CaptureFixture[str], tmp_path: Path) -> None:
    module = _load_module()
    coverage_path = tmp_path / "coverage.xml"
    xml = """<?xml version="1.0" ?>
<coverage>
  <packages>
    <package name="benchbox.cli">
      <classes>
        <class filename="/tmp/work/repo/benchbox/cli/a.py">
          <lines>
            <line number="1" hits="1" />
            <line number="2" hits="0" />
          </lines>
        </class>
      </classes>
    </package>
  </packages>
</coverage>
"""
    coverage_path.write_text(xml, encoding="utf-8")

    args = module.parse_args(["--coverage-xml", str(coverage_path), "--threshold", "10"])
    code = module.run(args)
    captured = capsys.readouterr()
    assert code == 0
    assert "benchbox/cli" in captured.out


def test_exclusions_loaded_from_pyproject(capsys: pytest.CaptureFixture[str], tmp_path: Path) -> None:
    module = _load_module()
    coverage_path = tmp_path / "coverage.xml"
    pyproject_path = tmp_path / "pyproject.toml"
    _write_coverage_xml(coverage_path)
    pyproject_path.write_text(
        """
[tool.benchbox.coverage.module_check]
exclude_modules = ["benchbox/core"]
""".strip()
        + "\n",
        encoding="utf-8",
    )

    args = module.parse_args(
        ["--coverage-xml", str(coverage_path), "--threshold", "70", "--exclude-from-pyproject", str(pyproject_path)]
    )
    code = module.run(args)
    captured = capsys.readouterr()
    assert code == 0
    assert "benchbox/cli" in captured.out
    assert "benchbox/core" not in captured.out
