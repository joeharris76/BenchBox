"""Tests for dependency validation utilities."""

import sys
from pathlib import Path

import pytest

pytestmark = [
    pytest.mark.unit,
    pytest.mark.medium,
]


if sys.version_info >= (3, 11):
    import tomllib
else:
    import tomli as tomllib  # type: ignore[import-not-found]

from benchbox.utils import dependency_validation


@pytest.mark.unit
class TestDependencyValidation:
    """Ensure dependency validation remains aligned with the lock file."""

    pyproject_path = Path("pyproject.toml")
    lock_path = Path("uv.lock")

    def test_validate_dependency_versions_matches_lock(self):
        pyproject_data = tomllib.loads(self.pyproject_path.read_text())
        lock_data = tomllib.loads(self.lock_path.read_text())

        problems = dependency_validation.validate_dependency_versions(pyproject_data, lock_data)

        assert problems == []

    def test_cli_matrix_output(self, capsys, monkeypatch):
        import benchbox.utils.printing as printing

        # Reset both globals: _QUIET may be leaked as True by CLI tests that call
        # set_quiet(True) via the --quiet flag but never restore it afterwards.
        monkeypatch.setattr(printing, "_QUIET", False)
        monkeypatch.setattr(printing, "_STD_CONSOLE", None)
        exit_code = dependency_validation.main(
            ["--matrix", "--pyproject", str(self.pyproject_path), "--lock", str(self.lock_path)]
        )

        captured = capsys.readouterr()
        assert exit_code == 0
        assert "Python compatibility" in captured.out
        assert "Optional dependency groups" in captured.out
        assert captured.err == ""
