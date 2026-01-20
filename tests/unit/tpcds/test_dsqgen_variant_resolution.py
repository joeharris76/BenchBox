"""Tests for TPC-DS dsqgen variant template resolution.

Ensures dsqgen is invoked with the correct -DIRECTORY (main templates dir)
and a relative -TEMPLATE path pointing into query_variants/ when a variant
template exists only under that subdirectory.
"""

from pathlib import Path

import pytest

from benchbox.core.tpcds.c_tools import DSQGenBinary

pytestmark = pytest.mark.fast


def _fake_completed(stdout: str = "SELECT 1\n"):
    class _CP:
        def __init__(self):
            self.returncode = 0
            self.stdout = stdout
            self.stderr = ""

    return _CP()


def test_dsqgen_uses_main_directory_for_variants(tmp_path, monkeypatch):
    # Arrange: create a fake templates structure
    templates_dir = tmp_path / "query_templates"
    variants_dir = tmp_path / "query_variants"
    templates_dir.mkdir(parents=True)
    variants_dir.mkdir(parents=True)

    # Minimal required files
    (templates_dir / "templates.lst").write_text("\n")
    (templates_dir / "ansi.tpl").write_text("-- dialect stub\n")

    # Base query exists in main; variant only in query_variants
    # Use query 1 instead of 14 since 14 is multi-part and variants are skipped for multi-part queries
    (templates_dir / "query1.tpl").write_text("-- base query 1\nSELECT 1;\n")
    (variants_dir / "query1a.tpl").write_text("-- variant query 1a\nSELECT 1;\n")

    # Instantiate dsqgen binary and point it to our fake templates
    dsq = DSQGenBinary()
    dsq.templates_dir = templates_dir

    captured_cmd: list[str] = []

    def fake_run(
        cmd,
        cwd=None,
        env=None,
        capture_output=None,
        text=None,
        timeout=None,
        check=None,
        stdout=None,
        stderr=None,
    ):
        nonlocal captured_cmd
        captured_cmd = list(cmd)
        # Query 1 is a single-part query (not multi-part)
        return _fake_completed(stdout="SELECT 1;\n")

    monkeypatch.setattr("subprocess.run", fake_run)

    # Act: generate a variant query by passing composite id '1a'
    sql = dsq.generate("1a", seed=22, scale_factor=1.0, dialect="ansi")

    # Assert: command uses templates_dir for -DIRECTORY and relative -TEMPLATE with ../ prefix for variants
    # Extract args
    assert "-DIRECTORY" in captured_cmd
    dir_idx = captured_cmd.index("-DIRECTORY") + 1
    tmpl_dir = Path(captured_cmd[dir_idx])
    # Should be the query_templates directory (not parent)
    assert tmpl_dir == templates_dir

    assert "-TEMPLATE" in captured_cmd
    tpl_idx = captured_cmd.index("-TEMPLATE") + 1
    tpl_arg = captured_cmd[tpl_idx]
    # For variants, the template path should use ../ to access sibling directory
    assert tpl_arg == "../query_variants/query1a.tpl"

    # validate output path goes through and returns SQL
    assert "select" in sql.lower()


def test_validate_query_id_accepts_variant_in_query_variants(tmp_path):
    templates_dir = tmp_path / "query_templates"
    variants_dir = tmp_path / "query_variants"
    templates_dir.mkdir(parents=True)
    variants_dir.mkdir(parents=True)

    (templates_dir / "templates.lst").write_text("\n")
    (templates_dir / "ansi.tpl").write_text("-- dialect\n")
    (templates_dir / "query14.tpl").write_text("-- base 14\n")
    (variants_dir / "query14a.tpl").write_text("-- variant 14a\n")

    dsq = DSQGenBinary()
    dsq.templates_dir = templates_dir

    assert dsq.validate_query_id("14a") is True
