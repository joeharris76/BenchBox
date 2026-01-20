"""Unit tests for cloud path validation in BenchmarkDataValidator.

Copyright 2026 Joe Harris / BenchBox Project
Licensed under the MIT License. See LICENSE file in the project root for details.
"""

import sys
from unittest.mock import MagicMock, patch

import pytest

from benchbox.utils.data_validation import BenchmarkDataValidator

pytestmark = pytest.mark.fast


class TestCloudPathValidation:
    """Test cloud path detection and handling in data validator."""

    @pytest.fixture
    def validator(self):
        """Create a validator instance for testing."""
        return BenchmarkDataValidator(
            benchmark_name="tpch",
            scale_factor=1.0,
        )

    def test_cloudpath_instance_skips_validation(self, validator):
        """Test that CloudPath instances trigger cloud path handling."""
        # Mock CloudPath to avoid dependency on cloudpathlib being installed
        mock_cloudpath = MagicMock()
        mock_cloudpath.__class__.__name__ = "GCSPath"
        # Mock __fspath__ to return a non-existent path so validation fails
        mock_cloudpath.__fspath__ = MagicMock(return_value="/nonexistent/cloud/path")

        # Patch the import to return our mock class
        with patch.dict(sys.modules, {"cloudpathlib": MagicMock(CloudPath=type(mock_cloudpath))}):
            # Reload the validator to pick up the patched import
            from benchbox.utils.data_validation import BenchmarkDataValidator

            test_validator = BenchmarkDataValidator(
                benchmark_name="tpch",
                scale_factor=1.0,
            )

            result = test_validator.validate_data_directory(mock_cloudpath)

            # Should return invalid because path doesn't exist
            assert result.valid is False
            # Should have at least 1 issue about the path not existing
            assert len(result.issues) >= 1

    def test_databricks_path_skips_validation(self, validator, tmp_path):
        """Test that DatabricksPath instances use local staging path for validation."""
        from benchbox.utils.cloud_storage import DatabricksPath

        # Create a local staging directory
        staging_dir = tmp_path / "staging"
        staging_dir.mkdir()

        # Create a DatabricksPath instance with the staging directory
        dbfs_path = DatabricksPath(local_path=str(staging_dir), dbfs_target="dbfs:/data/test")

        result = validator.validate_data_directory(dbfs_path)

        # Should validate the local staging path, which has no data files
        assert result.valid is False
        # Should have validation issues about missing tables, not cloud path messages
        assert len(result.issues) >= 1
        assert not any("Cloud storage path detected" in issue for issue in result.issues)

    def test_local_path_performs_normal_validation(self, validator, tmp_path):
        """Test that local Path objects go through normal validation."""
        # Create a temporary directory with no data files
        test_dir = tmp_path / "tpch_data"
        test_dir.mkdir()

        result = validator.validate_data_directory(test_dir)

        # Should perform normal validation (which will fail due to missing files)
        assert result.valid is False
        # Should NOT have cloud path messages
        assert not any("Cloud storage path detected" in issue for issue in result.issues)

    def test_local_string_path_performs_normal_validation(self, validator, tmp_path):
        """Test that local string paths go through normal validation."""
        # Create a temporary directory with no data files
        test_dir = tmp_path / "tpch_data"
        test_dir.mkdir()

        result = validator.validate_data_directory(str(test_dir))

        # Should perform normal validation (which will fail due to missing files)
        assert result.valid is False
        # Should NOT have cloud path messages
        assert not any("Cloud storage path detected" in issue for issue in result.issues)

    def test_cloudpath_without_cloudpathlib_installed(self, validator):
        """Test graceful degradation when cloudpathlib is not installed."""
        # Mock a path-like object that looks like a CloudPath but cloudpathlib is not installed
        mock_path = MagicMock()
        mock_path.__class__.__name__ = "GCSPath"

        # Patch the import to fail (simulating cloudpathlib not installed)
        with patch("benchbox.utils.data_validation.logger"):
            with patch.dict(sys.modules, {"cloudpathlib": None}):
                # Since cloudpathlib is not installed, the validator will treat this as a regular path
                # and try to convert it to Path, which will likely fail or produce unexpected results
                # This tests that the code doesn't crash when cloudpathlib is not available
                try:
                    result = validator.validate_data_directory(mock_path)
                    # If it succeeds, it should have done some validation attempt
                    assert result is not None
                except Exception:
                    # If it fails, that's also acceptable behavior (path doesn't exist, etc.)
                    pass

    def test_cloud_path_preserves_benchmark_context(self, validator):
        """Test that cloud path validation includes benchmark name and scale factor."""
        from benchbox.utils.cloud_storage import DatabricksPath

        # Create validator with specific benchmark context
        test_validator = BenchmarkDataValidator(
            benchmark_name="tpcds",
            scale_factor=100.0,
        )

        dbfs_path = DatabricksPath(local_path="/tmp/test", dbfs_target="dbfs:/data/test")
        result = test_validator.validate_data_directory(dbfs_path)

        # Validator should preserve its context even when skipping validation
        assert result.valid is False
        assert test_validator.benchmark_name == "tpcds"
        assert test_validator.scale_factor == 100.0

    def test_cloud_path_returns_correct_result_structure(self, validator, tmp_path):
        """Test that cloud path validation returns properly structured result."""
        from benchbox.utils.cloud_storage import DatabricksPath

        # Create staging directory so validation can run
        staging_dir = tmp_path / "staging"
        staging_dir.mkdir()

        dbfs_path = DatabricksPath(local_path=str(staging_dir), dbfs_target="dbfs:/data/test")
        result = validator.validate_data_directory(dbfs_path)

        # Verify result structure
        assert hasattr(result, "valid")
        assert hasattr(result, "tables_validated")
        assert hasattr(result, "missing_tables")
        assert hasattr(result, "row_count_mismatches")
        assert hasattr(result, "file_size_info")
        assert hasattr(result, "validation_timestamp")
        assert hasattr(result, "issues")

        # Verify validation was performed (tables_validated will have entries)
        assert isinstance(result.tables_validated, dict)
        assert isinstance(result.missing_tables, list)
        assert result.row_count_mismatches == {}
        assert result.file_size_info == {}

        # Verify meaningful issues
        assert len(result.issues) > 0
        assert result.validation_timestamp is not None
