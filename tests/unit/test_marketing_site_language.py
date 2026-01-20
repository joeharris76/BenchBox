"""Tests for marketing site messaging accuracy."""

from pathlib import Path

import pytest

INDEX_PATH = Path("index.html")


@pytest.mark.fast
@pytest.mark.unit
@pytest.mark.skipif(not INDEX_PATH.exists(), reason="index.html not present (not in public release)")
def test_index_page_describes_dependencies_accurately():
    """Ensure landing page messaging does not claim zero dependencies."""
    contents = INDEX_PATH.read_text(encoding="utf-8")

    assert "Zero Dependencies" not in contents
    assert "Run Your Own Benchmarks" in contents
    assert "Python-Only Core" in contents
    assert "optional extras" in contents.lower()
