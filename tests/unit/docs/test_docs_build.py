"""Ensure the Sphinx documentation builds after structural changes."""

from __future__ import annotations

from pathlib import Path

import pytest

pytestmark = pytest.mark.medium  # Sphinx build takes 5-6s

try:
    from sphinx.cmd.build import build_main
except ImportError:  # pragma: no cover - handled at runtime
    build_main = None  # type: ignore

try:
    import sphinxcontrib.mermaid  # noqa: F401

    has_mermaid = True
except ImportError:  # pragma: no cover - handled at runtime
    has_mermaid = False


@pytest.mark.skipif(build_main is None, reason="Sphinx is not installed in the test environment")
@pytest.mark.skipif(not has_mermaid, reason="Sphinx extension missing: sphinxcontrib.mermaid not installed")
def test_sphinx_docs_build(tmp_path: Path) -> None:
    """Build the docs with the dummy builder to validate the navigation graph."""

    docs_dir = Path(__file__).resolve().parents[3] / "docs"
    output_dir = tmp_path / "_build"

    result = build_main(["-n", "-b", "dummy", str(docs_dir), str(output_dir)])
    assert result == 0, "Sphinx reported errors while building the documentation"
