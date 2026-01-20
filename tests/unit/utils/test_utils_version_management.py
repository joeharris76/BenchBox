"""Tests for version management utilities."""

import pytest

from benchbox.utils import version as version_utils


@pytest.mark.unit
@pytest.mark.fast
class TestVersionConsistency:
    """Validate version consistency helpers."""

    def test_check_version_consistency_matches_pyproject(self):
        """The recorded versions should match between package and pyproject."""
        result = version_utils.check_version_consistency()

        assert result.consistent, result.message
        assert result.sources["benchbox.__init__"] == result.sources["pyproject.toml"]

    def test_check_version_consistency_detects_mismatch(self, monkeypatch):
        """A mismatch between sources must be reported."""

        monkeypatch.setattr(version_utils, "get_pyproject_version", lambda: "9.9.9")

        result = version_utils.check_version_consistency()

        assert not result.consistent
        assert "mismatch" in result.message.lower()
        assert result.sources["pyproject.toml"] == "9.9.9"


@pytest.mark.unit
@pytest.mark.fast
class TestVersionCompatibility:
    """Check semantic version compatibility helpers."""

    def test_is_version_compatible_within_bounds(self):
        """Current version should always be compatible with its own bounds."""
        current = version_utils.get_package_version()

        assert version_utils.is_version_compatible(min_version=current)
        assert version_utils.is_version_compatible(max_version=current)

    def test_is_version_compatible_out_of_bounds(self):
        """Compatibility check should fail when outside the supported range."""
        assert not version_utils.is_version_compatible(max_version="0.0.1")

    def test_is_version_compatible_invalid_version_string(self):
        """Invalid spec strings should raise ValueError for quick feedback."""
        with pytest.raises(ValueError):
            version_utils.is_version_compatible(min_version="not-a-version")

    def test_ensure_version_compatible_raises_with_clear_message(self):
        """ensure_version_compatible should raise when the range is violated."""
        with pytest.raises(RuntimeError) as excinfo:
            version_utils.ensure_version_compatible(max_version="0.0.1")

        assert "compatibility" in str(excinfo.value).lower()


@pytest.mark.unit
@pytest.mark.fast
class TestEnhancedImportErrors:
    """Validate helpful messaging when optional dependencies are missing."""

    def test_create_import_error_includes_dependency_guidance(self):
        """Import errors must include actionable installation guidance."""
        error = version_utils.create_import_error(
            benchmark_name="TPCDI",
            missing_dependencies=["tpcdi", "extras"],
            original_error=ImportError("No module named benchbox.tpcdi"),
        )

        message = str(error)

        assert "uv add" in message
        assert "Version Information" in message
        assert "Original error" in message
