"""Tests to ensure Sphinx documentation builds without critical warnings."""

import subprocess
import tempfile
from pathlib import Path

import pytest


def test_sphinx_build_specific_warning_types():
    """
    Test that specific types of warnings are zero.

    This test explicitly checks for warning categories that indicate problems:
    - autodoc import failures
    - nonexisting documents in toctrees
    - unknown document references
    - orphaned documents
    """
    docs_dir = Path(__file__).parent.parent.parent / "docs"

    with tempfile.TemporaryDirectory() as tmpdir:
        build_dir = Path(tmpdir) / "_build/html"

        result = subprocess.run(
            [
                "uv",
                "run",
                "python",
                "-m",
                "sphinx",
                "-b",
                "html",
                str(docs_dir),
                str(build_dir),
            ],
            capture_output=True,
            text=True,
            timeout=180,
        )

        output = result.stderr + result.stdout

        # Check for specific problem patterns
        problematic_patterns = {
            "autodoc import failures": "failed to import",
            "nonexisting documents": "toctree contains reference to nonexisting document",
            "orphaned documents": "document isn't included in any toctree",
            "unknown documents": "unknown document:",
            "broken references": "undefined label:",
        }

        errors = []
        for name, pattern in problematic_patterns.items():
            count = output.lower().count(pattern.lower())
            if count > 0:
                errors.append(f"{name}: {count} occurrences of '{pattern}'")

        if errors:
            pytest.fail("Found problematic warnings:\n" + "\n".join(errors))


if __name__ == "__main__":
    # Allow running directly for debugging
    test_sphinx_build_specific_warning_types()
    print("All Sphinx warning tests passed!")
