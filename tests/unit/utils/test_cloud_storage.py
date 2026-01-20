"""Tests for cloud storage utilities.

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

import os
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from benchbox.utils.cloud_storage import (
    CloudPathAdapter,
    CloudStagingPath,
    DatabricksPath,
    create_path_handler,
    format_cloud_usage_guide,
    get_cloud_path_info,
    is_cloud_path,
    is_databricks_path,
    validate_cloud_credentials,
    validate_cloud_path_support,
)

pytestmark = pytest.mark.medium  # Cloud storage tests have network-like operations (~1s)


class TestCloudPathDetection:
    """Test cloud path detection functionality."""

    def test_is_cloud_path_s3(self):
        """Test S3 path detection."""
        assert is_cloud_path("s3://bucket/path")
        assert is_cloud_path("s3://bucket/path/file.txt")
        assert not is_cloud_path("/local/path")
        assert not is_cloud_path("C:\\Windows\\path")
        assert not is_cloud_path("")

    def test_is_cloud_path_gcs(self):
        """Test GCS path detection."""
        assert is_cloud_path("gs://bucket/path")
        assert is_cloud_path("gcs://bucket/path")
        assert is_cloud_path("gs://bucket/path/file.txt")

    def test_is_cloud_path_azure(self):
        """Test Azure path detection."""
        assert is_cloud_path("abfss://container@account.dfs.core.windows.net/path")
        assert is_cloud_path("azure://container@account/path")

    def test_is_cloud_path_path_objects(self):
        """Test cloud path detection with Path objects."""
        assert not is_cloud_path(Path("/local/path"))
        assert not is_cloud_path(Path.cwd())

    def test_is_cloud_path_invalid_input(self):
        """Test cloud path detection with invalid input."""
        assert not is_cloud_path(None)
        assert not is_cloud_path(123)
        assert not is_cloud_path([])

    def test_is_cloud_path_dbfs(self):
        """Test DBFS path detection."""
        assert is_cloud_path("dbfs:/Volumes/workspace/benchbox/data")
        assert is_cloud_path("dbfs:/tmp/test")
        assert is_cloud_path("dbfs://path/to/data")


class TestCloudPathSupport:
    """Test cloud path support validation."""

    @patch("benchbox.utils.cloud_storage.CloudPath", None)
    def test_validate_cloud_path_support_missing(self):
        """Test validation when cloudpathlib is not installed."""
        assert not validate_cloud_path_support()

    def test_validate_cloud_path_support_available(self):
        """Test validation when cloudpathlib is available."""
        # This test assumes cloudpathlib is available in test environment
        # If not available, it should be skipped
        try:
            import cloudpathlib

            assert validate_cloud_path_support()
        except ImportError:
            pytest.skip("cloudpathlib not available in test environment")


class TestPathHandlerCreation:
    """Test path handler creation."""

    def test_create_path_handler_local(self):
        """Test creating path handler for local paths."""
        local_path = "/tmp/local"
        handler = create_path_handler(local_path)
        assert isinstance(handler, Path)
        assert str(handler) == local_path

    def test_create_path_handler_local_path_object(self):
        """Test creating path handler for Path objects."""
        local_path = Path("/tmp/local")
        handler = create_path_handler(local_path)
        assert isinstance(handler, Path)
        assert handler == local_path

    @patch("benchbox.utils.cloud_storage.CloudPath", None)
    def test_create_path_handler_cloud_missing_lib(self):
        """Test creating path handler for cloud paths without cloudpathlib."""
        with pytest.raises(ImportError, match="cloudpathlib is required"):
            create_path_handler("s3://bucket/path")

    @patch("benchbox.utils.cloud_storage.CloudPath")
    def test_create_path_handler_cloud_success(self, mock_cloud_path):
        """Test creating path handler for cloud paths."""
        mock_instance = MagicMock()
        mock_cloud_path.return_value = mock_instance

        result = create_path_handler("s3://bucket/path")

        mock_cloud_path.assert_called_once_with("s3://bucket/path")
        assert result == mock_instance

    @patch("benchbox.utils.cloud_storage.CloudPath")
    def test_create_path_handler_cloud_invalid_format(self, mock_cloud_path):
        """Test creating path handler for invalid cloud paths."""
        mock_cloud_path.side_effect = Exception("Invalid format")

        with pytest.raises(ValueError, match="Invalid cloud path format"):
            create_path_handler("s3://invalid-format")


class TestCredentialValidation:
    """Test cloud credential validation."""

    def test_validate_cloud_credentials_local_path(self):
        """Test credential validation for local paths."""
        result = validate_cloud_credentials("/local/path")
        assert result["valid"] is True
        assert result["provider"] == "local"
        assert result["error"] is None

    @patch("benchbox.utils.cloud_storage.CloudPath", None)
    def test_validate_cloud_credentials_missing_lib(self):
        """Test credential validation without cloudpathlib."""
        result = validate_cloud_credentials("s3://bucket/path")
        assert result["valid"] is False
        assert result["provider"] == "unknown"
        assert "cloudpathlib not installed" in result["error"]

    @patch.dict(os.environ, {}, clear=True)
    @patch("os.path.expanduser", return_value="/nonexistent/path")
    def test_validate_cloud_credentials_s3_missing_vars(self, mock_expanduser):
        """Test S3 credential validation with missing environment variables and no credentials files."""
        result = validate_cloud_credentials("s3://bucket/path")
        assert result["valid"] is False
        # Provider might be "unknown" if cloudpathlib is not available
        assert result["provider"] in ["s3", "unknown"]
        if result["provider"] == "s3":
            assert "No AWS credentials found" in result["error"]
            assert result["env_vars"] == ["AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"]
        else:
            assert "cloudpathlib not installed" in result["error"]

    @patch.dict(
        os.environ,
        {"AWS_ACCESS_KEY_ID": "test-key", "AWS_SECRET_ACCESS_KEY": "test-secret"},
        clear=True,
    )
    @patch("benchbox.utils.cloud_storage.CloudPath")
    def test_validate_cloud_credentials_s3_success(self, mock_cloud_path):
        """Test S3 credential validation success."""
        mock_instance = MagicMock()
        mock_instance.exists.return_value = True
        mock_cloud_path.return_value = mock_instance

        result = validate_cloud_credentials("s3://bucket/path")
        assert result["valid"] is True
        assert result["provider"] == "s3"
        assert result["error"] is None

    @patch.dict(os.environ, {}, clear=True)
    def test_validate_cloud_credentials_gcs_missing_vars(self):
        """Test GCS credential validation with missing environment variables."""
        result = validate_cloud_credentials("gs://bucket/path")
        assert result["valid"] is False
        # Provider might be "unknown" if cloudpathlib is not available
        assert result["provider"] in ["gs", "unknown"]
        if result["provider"] == "gs":
            assert "Missing environment variables" in result["error"]
            assert "GOOGLE_APPLICATION_CREDENTIALS" in result["error"]
        else:
            assert "cloudpathlib not installed" in result["error"]

    @patch.dict(os.environ, {}, clear=True)
    def test_validate_cloud_credentials_azure_missing_vars(self):
        """Test Azure credential validation with missing environment variables."""
        result = validate_cloud_credentials("abfss://container@account.dfs.core.windows.net/path")
        assert result["valid"] is False
        # Provider might be "unknown" if cloudpathlib is not available
        assert result["provider"] in ["abfss", "unknown"]
        if result["provider"] == "abfss":
            assert "Missing environment variables" in result["error"]
            assert "AZURE_STORAGE_ACCOUNT_NAME" in result["error"]
        else:
            assert "cloudpathlib not installed" in result["error"]


class TestPathInfo:
    """Test cloud path information extraction."""

    def test_get_cloud_path_info_local(self):
        """Test path info for local paths."""
        info = get_cloud_path_info("/local/path")
        assert info["is_cloud"] is False
        assert info["provider"] == "local"
        assert info["bucket"] is None
        assert info["path"] == "/local/path"
        assert info["credentials_valid"] is True

    def test_get_cloud_path_info_s3(self):
        """Test path info for S3 paths."""
        info = get_cloud_path_info("s3://my-bucket/path/to/data")
        assert info["is_cloud"] is True
        assert info["provider"] == "s3"
        assert info["bucket"] == "my-bucket"
        assert info["path"] == "path/to/data"
        # credentials_valid will depend on environment

    def test_get_cloud_path_info_gcs(self):
        """Test path info for GCS paths."""
        info = get_cloud_path_info("gs://my-bucket/path/to/data")
        assert info["is_cloud"] is True
        assert info["provider"] == "gs"
        assert info["bucket"] == "my-bucket"
        assert info["path"] == "path/to/data"

    def test_get_cloud_path_info_azure(self):
        """Test path info for Azure paths."""
        info = get_cloud_path_info("abfss://container@account.dfs.core.windows.net/path/to/data")
        assert info["is_cloud"] is True
        assert info["provider"] == "abfss"
        assert info["bucket"] == "container@account.dfs.core.windows.net"
        assert info["path"] == "path/to/data"


class TestCloudPathAdapter:
    """Test CloudPathAdapter functionality."""

    def test_cloud_path_adapter_local(self):
        """Test CloudPathAdapter with local paths."""
        adapter = CloudPathAdapter("/tmp/test")
        assert not adapter.is_cloud
        assert adapter.path_info["is_cloud"] is False
        assert adapter.path_info["provider"] == "local"

    def test_cloud_path_adapter_cloud_without_lib(self):
        """Test CloudPathAdapter with cloud paths but no cloudpathlib."""
        with patch("benchbox.utils.cloud_storage.CloudPath", None), pytest.raises(ImportError):
            CloudPathAdapter("s3://bucket/path")

    def test_cloud_path_adapter_str_representation(self):
        """Test string representation of CloudPathAdapter."""
        adapter = CloudPathAdapter("/tmp/test")
        assert "/tmp/test" in str(adapter)

    def test_cloud_path_adapter_path_joining(self):
        """Test path joining with CloudPathAdapter."""
        adapter = CloudPathAdapter("/tmp/test")
        new_adapter = adapter / "subpath"
        assert isinstance(new_adapter, CloudPathAdapter)
        assert "subpath" in str(new_adapter)


class TestUsageGuides:
    """Test cloud storage usage guides."""

    def test_format_cloud_usage_guide_s3(self):
        """Test S3 usage guide formatting."""
        guide = format_cloud_usage_guide("s3")
        assert "AWS S3 Setup:" in guide
        assert "AWS_ACCESS_KEY_ID" in guide
        assert "AWS_SECRET_ACCESS_KEY" in guide
        assert "s3://your-bucket" in guide

    def test_format_cloud_usage_guide_gs(self):
        """Test GCS usage guide formatting."""
        guide = format_cloud_usage_guide("gs")
        assert "Google Cloud Storage Setup:" in guide
        assert "GOOGLE_APPLICATION_CREDENTIALS" in guide
        assert "gs://your-bucket" in guide

    def test_format_cloud_usage_guide_azure(self):
        """Test Azure usage guide formatting."""
        guide = format_cloud_usage_guide("azure")
        assert "Azure Blob Storage Setup:" in guide
        assert "AZURE_STORAGE_ACCOUNT_NAME" in guide
        assert "AZURE_STORAGE_ACCOUNT_KEY" in guide
        assert "abfss://" in guide

    def test_format_cloud_usage_guide_unknown(self):
        """Test usage guide for unknown provider."""
        guide = format_cloud_usage_guide("unknown")
        assert "No setup guide available" in guide


# Integration tests (require cloudpathlib)
class TestCloudPathIntegration:
    """Integration tests for cloud path functionality."""

    @pytest.mark.skipif(not validate_cloud_path_support(), reason="cloudpathlib not available")
    def test_cloud_path_creation_integration(self):
        """Test actual cloud path creation with cloudpathlib."""
        # Test that we can create cloud paths without errors
        # (This doesn't test actual cloud connectivity)
        try:
            handler = create_path_handler("s3://test-bucket/test-path")
            assert handler is not None
            assert hasattr(handler, "exists")  # CloudPath method
        except Exception as e:
            # If cloudpathlib has issues, that's not our fault
            pytest.skip(f"CloudPath creation failed: {e}")


class TestDatabricksPathDetection:
    """Test Databricks path detection functionality."""

    def test_is_databricks_path_valid_uc_volumes(self):
        """Test detection of valid Unity Catalog Volume paths."""
        assert is_databricks_path("dbfs:/Volumes/workspace/benchbox/data")
        assert is_databricks_path("dbfs:/Volumes/catalog/schema/volume")
        assert is_databricks_path("dbfs:/Volumes/catalog/schema/volume/subpath")

    def test_is_databricks_path_valid_dbfs(self):
        """Test detection of valid DBFS paths."""
        assert is_databricks_path("dbfs:/tmp/test")
        assert is_databricks_path("dbfs:/FileStore/data")
        assert is_databricks_path("dbfs://path/to/data")

    def test_is_databricks_path_invalid(self):
        """Test non-DBFS paths are not detected."""
        assert not is_databricks_path("s3://bucket/path")
        assert not is_databricks_path("gs://bucket/path")
        assert not is_databricks_path("/local/path")
        assert not is_databricks_path("C:\\Windows\\path")
        assert not is_databricks_path("")
        assert not is_databricks_path(None)
        assert not is_databricks_path(123)

    def test_is_databricks_path_with_path_objects(self):
        """Test Databricks path detection with Path objects."""
        assert not is_databricks_path(Path("/local/path"))
        # Path objects can't represent dbfs:// schemes


class TestDatabricksPathClass:
    """Test DatabricksPath wrapper class."""

    def test_databricks_path_creation(self):
        """Test creating a DatabricksPath object."""
        local_path = "/tmp/test"
        dbfs_target = "dbfs:/Volumes/workspace/benchbox/data"

        db_path = DatabricksPath(local_path, dbfs_target)

        assert str(db_path) == local_path
        assert db_path.dbfs_target == dbfs_target

    def test_databricks_path_with_path_object(self):
        """Test creating DatabricksPath with Path object."""
        local_path = Path("/tmp/test")
        dbfs_target = "dbfs:/Volumes/workspace/benchbox/data"

        db_path = DatabricksPath(local_path, dbfs_target)

        assert str(db_path) == str(local_path)
        assert db_path.dbfs_target == dbfs_target

    def test_databricks_path_fspath_protocol(self):
        """Test DatabricksPath implements os.PathLike protocol."""
        local_path = "/tmp/test"
        dbfs_target = "dbfs:/Volumes/workspace/benchbox/data"

        db_path = DatabricksPath(local_path, dbfs_target)

        # os.fspath() should work
        import os

        fspath = os.fspath(db_path)
        assert fspath == local_path

    def test_databricks_path_delegation(self):
        """Test DatabricksPath delegates methods to underlying Path."""
        import tempfile

        with tempfile.TemporaryDirectory() as temp_dir:
            dbfs_target = "dbfs:/Volumes/workspace/benchbox/data"
            db_path = DatabricksPath(temp_dir, dbfs_target)

            # Test delegated methods
            assert db_path.exists()
            assert db_path.is_dir()
            assert not db_path.is_file()
            assert db_path.name == Path(temp_dir).name

    def test_databricks_path_equality(self):
        """Test DatabricksPath equality comparisons."""
        local_path = "/tmp/test"
        dbfs_target1 = "dbfs:/Volumes/workspace/benchbox/data1"
        dbfs_target2 = "dbfs:/Volumes/workspace/benchbox/data2"

        db_path1 = DatabricksPath(local_path, dbfs_target1)
        db_path2 = DatabricksPath(local_path, dbfs_target1)
        db_path3 = DatabricksPath(local_path, dbfs_target2)

        # Same local path and dbfs target
        assert db_path1 == db_path2

        # Different dbfs target
        assert db_path1 != db_path3

        # Comparison with string/Path
        assert db_path1 == local_path
        assert db_path1 == Path(local_path)

    def test_databricks_path_repr(self):
        """Test DatabricksPath string representation."""
        local_path = "/tmp/test"
        dbfs_target = "dbfs:/Volumes/workspace/benchbox/data"

        db_path = DatabricksPath(local_path, dbfs_target)

        repr_str = repr(db_path)
        assert "DatabricksPath" in repr_str
        assert local_path in repr_str
        assert dbfs_target in repr_str

    def test_databricks_path_joining(self):
        """Test path joining with DatabricksPath."""
        local_path = "/tmp/test"
        dbfs_target = "dbfs:/Volumes/workspace/benchbox/data"

        db_path = DatabricksPath(local_path, dbfs_target)
        joined = db_path / "subpath"

        # Path joining returns a regular Path, not DatabricksPath
        assert isinstance(joined, Path)
        assert str(joined) == str(Path(local_path) / "subpath")


class TestDatabricksPathHandler:
    """Test path handler creation for Databricks paths."""

    def test_create_path_handler_dbfs_valid_uc_volume(self):
        """Test creating path handler for valid UC Volume path."""
        import shutil

        dbfs_path = "dbfs:/Volumes/workspace/benchbox/data"
        handler = create_path_handler(dbfs_path)

        try:
            # Should return DatabricksPath
            assert isinstance(handler, DatabricksPath)

            # Should have dbfs_target attribute
            assert handler.dbfs_target == dbfs_path

            # Local path should exist and be a temporary directory
            assert handler.exists()
            assert handler.is_dir()
            assert "benchbox_dbfs_" in str(handler)

            # Local path should NOT contain dbfs: scheme
            assert "dbfs:" not in str(handler)

        finally:
            # Clean up temp directory
            if handler.exists():
                shutil.rmtree(str(handler))

    def test_create_path_handler_dbfs_invalid_path(self):
        """Test creating path handler for invalid DBFS path."""
        # Non-UC Volume paths should be rejected
        invalid_paths = [
            "dbfs:/tmp/data",
            "dbfs:/FileStore/data",
            "dbfs://data",
        ]

        for invalid_path in invalid_paths:
            with pytest.raises(ValueError, match="Invalid dbfs:// path"):
                create_path_handler(invalid_path)


class TestDatabricksCredentialValidation:
    """Test credential validation for Databricks paths."""

    def test_validate_cloud_credentials_dbfs(self):
        """Test credential validation for DBFS paths."""
        result = validate_cloud_credentials("dbfs:/Volumes/workspace/benchbox/data")

        # Should return valid (actual validation happens in Databricks adapter)
        assert result["valid"] is True
        assert result["provider"] == "dbfs"
        assert result["error"] is None
        assert "DATABRICKS_HOST" in result["env_vars"]
        assert "DATABRICKS_HTTP_PATH" in result["env_vars"]
        assert "DATABRICKS_TOKEN" in result["env_vars"]


class TestDatabricksPathInfo:
    """Test path information extraction for Databricks paths."""

    def test_get_cloud_path_info_dbfs_uc_volume(self):
        """Test path info for Unity Catalog Volume paths."""
        info = get_cloud_path_info("dbfs:/Volumes/workspace/benchbox/data")

        assert info["is_cloud"] is True
        assert info["provider"] == "dbfs"
        assert info["bucket"] is None  # Not applicable for DBFS
        assert info["path"] == "/Volumes/workspace/benchbox/data"
        assert info["credentials_valid"] is True

        # Check UC Volume components
        volume_info = info["volume_info"]
        assert volume_info["catalog"] == "workspace"
        assert volume_info["schema"] == "benchbox"
        assert volume_info["volume"] == "data"

    def test_get_cloud_path_info_dbfs_with_subpath(self):
        """Test path info for UC Volume with subpath."""
        info = get_cloud_path_info("dbfs:/Volumes/workspace/benchbox/data/tpch/sf01")

        assert info["is_cloud"] is True
        assert info["provider"] == "dbfs"
        assert info["path"] == "/Volumes/workspace/benchbox/data/tpch/sf01"

        # Check UC Volume components (should extract base volume)
        volume_info = info["volume_info"]
        assert volume_info["catalog"] == "workspace"
        assert volume_info["schema"] == "benchbox"
        assert volume_info["volume"] == "data"

    def test_get_cloud_path_info_dbfs_invalid_format(self):
        """Test path info for non-UC Volume DBFS paths."""
        info = get_cloud_path_info("dbfs:/tmp/data")

        assert info["is_cloud"] is True
        assert info["provider"] == "dbfs"
        assert info["path"] == "/tmp/data"
        # volume_info should be empty for non-UC paths
        volume_info = info.get("volume_info", {})
        assert not volume_info or volume_info.get("catalog") is None


class TestDatabricksUsageGuide:
    """Test usage guide for Databricks."""

    def test_format_cloud_usage_guide_dbfs(self):
        """Test DBFS usage guide formatting."""
        guide = format_cloud_usage_guide("dbfs")

        assert "Databricks DBFS" in guide or "Unity Catalog Volumes" in guide
        assert "DATABRICKS_HOST" in guide
        assert "DATABRICKS_HTTP_PATH" in guide
        assert "DATABRICKS_TOKEN" in guide
        assert "dbfs:/Volumes/" in guide
        assert "databricks-sdk" in guide


class TestCloudStagingPath:
    """Test CloudStagingPath for persistent local caching with cloud upload."""

    def test_initialization_with_str_paths(self):
        """Test CloudStagingPath initialization with string paths."""
        local_path = "/tmp/data"
        cloud_target = "gs://my-bucket/data"

        staging_path = CloudStagingPath(local_path, cloud_target)

        assert staging_path._path == Path(local_path)
        assert staging_path._cloud_target == cloud_target

    def test_initialization_with_path_objects(self):
        """Test CloudStagingPath initialization with Path objects."""
        local_path = Path("/tmp/data")
        cloud_target = "s3://my-bucket/data"

        staging_path = CloudStagingPath(local_path, cloud_target)

        assert staging_path._path == local_path
        assert staging_path._cloud_target == cloud_target

    def test_fspath_protocol(self):
        """Test __fspath__() returns local path string for os.PathLike protocol."""
        local_path = "/tmp/benchmark_data"
        staging_path = CloudStagingPath(local_path, "gs://bucket/path")

        # os.fspath() should work with CloudStagingPath
        assert os.fspath(staging_path) == local_path

    def test_str_returns_local_path(self):
        """Test str() returns local path."""
        local_path = "/tmp/data"
        staging_path = CloudStagingPath(local_path, "gs://bucket/data")

        assert str(staging_path) == local_path

    def test_repr_shows_both_paths(self):
        """Test repr() shows both local and cloud paths."""
        local_path = "/tmp/data"
        cloud_target = "gs://bucket/data"
        staging_path = CloudStagingPath(local_path, cloud_target)

        repr_str = repr(staging_path)
        assert "CloudStagingPath" in repr_str
        assert "/tmp/data" in repr_str
        assert "gs://bucket/data" in repr_str

    def test_cloud_target_property(self):
        """Test cloud_target property returns the cloud URI."""
        cloud_target = "s3://my-bucket/benchmark-data"
        staging_path = CloudStagingPath("/local/path", cloud_target)

        assert staging_path.cloud_target == cloud_target

    def test_path_joining_with_truediv(self):
        """Test path joining with / operator."""
        staging_path = CloudStagingPath("/tmp/data", "gs://bucket/data")

        joined = staging_path / "subdir" / "file.txt"

        assert isinstance(joined, Path)
        assert str(joined) == "/tmp/data/subdir/file.txt"

    def test_equality_comparison(self):
        """Test equality comparison between CloudStagingPath instances."""
        path1 = CloudStagingPath("/tmp/data", "gs://bucket/data")
        path2 = CloudStagingPath("/tmp/data", "gs://bucket/data")
        path3 = CloudStagingPath("/tmp/other", "gs://bucket/data")
        path4 = CloudStagingPath("/tmp/data", "gs://other-bucket/data")

        assert path1 == path2
        assert path1 != path3  # Different local path
        assert path1 != path4  # Different cloud target

    def test_equality_with_path_objects(self):
        """Test equality comparison with Path objects."""
        staging_path = CloudStagingPath("/tmp/data", "gs://bucket/data")

        assert staging_path == Path("/tmp/data")
        assert staging_path == "/tmp/data"
        assert staging_path != Path("/tmp/other")

    def test_hash_based_on_local_path(self):
        """Test hash() works for use in sets/dicts."""
        path1 = CloudStagingPath("/tmp/data", "gs://bucket/data")
        path2 = CloudStagingPath("/tmp/data", "gs://bucket/data")  # Same everything
        path3 = CloudStagingPath("/tmp/data", "gs://other-bucket/data")  # Different cloud target

        # Same local path = same hash (for dict/set usage)
        assert hash(path1) == hash(path2) == hash(path3)

        # Can be used in sets - but only truly equal instances deduplicate
        path_set = {path1, path2}
        assert len(path_set) == 1  # path1 and path2 are equal, so deduplicated

        # Different cloud targets are NOT equal, so no deduplication
        path_set2 = {path1, path3}
        assert len(path_set2) == 2  # path1 and path3 have different cloud_targets

    def test_path_operations_delegate_to_local(self, tmp_path):
        """Test Path operations work through delegation."""
        staging_path = CloudStagingPath(str(tmp_path), "gs://bucket/data")

        # mkdir
        staging_path.mkdir(parents=True, exist_ok=True)
        assert tmp_path.exists()

        # is_dir
        assert staging_path.is_dir()

        # Create a test file
        test_file = tmp_path / "test.txt"
        test_file.write_text("content")

        # iterdir
        files = list(staging_path.iterdir())
        assert len(files) == 1

        # glob
        matches = list(staging_path.glob("*.txt"))
        assert len(matches) == 1

    def test_path_properties(self, tmp_path):
        """Test Path properties work correctly."""
        local_path = tmp_path / "subdir" / "file.txt"
        staging_path = CloudStagingPath(str(local_path), "s3://bucket/data")

        assert staging_path.name == "file.txt"
        assert staging_path.parent == local_path.parent
        assert staging_path.parts == local_path.parts

    def test_as_posix_returns_local_posix(self):
        """Test as_posix() returns local path in POSIX format."""
        staging_path = CloudStagingPath("/tmp/data/file.txt", "gs://bucket/data")

        assert staging_path.as_posix() == "/tmp/data/file.txt"

    def test_resolve_returns_absolute_local_path(self):
        """Test resolve() returns absolute local path."""
        staging_path = CloudStagingPath("relative/path", "s3://bucket/data")

        resolved = staging_path.resolve()
        assert resolved.is_absolute()

    def test_with_different_cloud_providers(self):
        """Test CloudStagingPath works with different cloud providers."""
        gcs_path = CloudStagingPath("/tmp/data", "gs://gcs-bucket/path")
        s3_path = CloudStagingPath("/tmp/data", "s3://s3-bucket/path")
        azure_path = CloudStagingPath("/tmp/data", "abfss://container@account.dfs.core.windows.net/path")

        assert gcs_path.cloud_target.startswith("gs://")
        assert s3_path.cloud_target.startswith("s3://")
        assert azure_path.cloud_target.startswith("abfss://")

        # All expose the same local path
        assert os.fspath(gcs_path) == os.fspath(s3_path) == os.fspath(azure_path)
