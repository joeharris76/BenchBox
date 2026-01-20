"""Tests for CloudSparkStaging unified cloud storage interface.

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from benchbox.platforms.base.cloud_spark.staging import (
    CloudProvider,
    CloudSparkStaging,
    LocalStaging,
    S3Staging,
    StagingConfig,
    UploadProgress,
)

pytestmark = pytest.mark.fast


class TestCloudProvider:
    """Test CloudProvider enum."""

    def test_provider_values(self):
        """Test all provider values are defined."""
        assert CloudProvider.AWS_S3.value == "s3"
        assert CloudProvider.GCS.value == "gs"
        assert CloudProvider.AZURE_ADLS.value == "abfss"
        assert CloudProvider.DBFS.value == "dbfs"
        assert CloudProvider.LOCAL.value == "file"


class TestUploadProgress:
    """Test UploadProgress dataclass."""

    def test_percent_complete(self):
        """Test percentage calculation."""
        progress = UploadProgress(
            table_name="lineitem",
            file_name="lineitem.parquet",
            bytes_uploaded=50,
            total_bytes=100,
            files_completed=1,
            total_files=5,
        )
        assert progress.percent_complete == 50.0

    def test_percent_complete_zero_total(self):
        """Test percentage with zero total bytes."""
        progress = UploadProgress(
            table_name="empty",
            file_name="empty.parquet",
            bytes_uploaded=0,
            total_bytes=0,
            files_completed=0,
            total_files=0,
        )
        assert progress.percent_complete == 100.0


class TestCloudSparkStagingFromUri:
    """Test CloudSparkStaging.from_uri() factory method."""

    def test_from_uri_s3(self):
        """Test S3 URI parsing."""
        staging = CloudSparkStaging.from_uri("s3://my-bucket/path/to/data")

        assert isinstance(staging, S3Staging)
        assert staging.config.provider == CloudProvider.AWS_S3
        assert staging.config.bucket == "my-bucket"
        assert staging.config.prefix == "path/to/data"

    def test_from_uri_s3a(self):
        """Test s3a:// scheme is treated as S3."""
        staging = CloudSparkStaging.from_uri("s3a://my-bucket/data")

        assert isinstance(staging, S3Staging)
        assert staging.config.provider == CloudProvider.AWS_S3

    def test_from_uri_gcs(self):
        """Test GCS URI parsing."""
        with patch(
            "benchbox.platforms.base.cloud_spark.staging.GCSStaging.__init__",
            return_value=None,
        ) as mock_init:
            CloudSparkStaging.from_uri("gs://my-bucket/data")
            # Verify __init__ was called with correct config
            call_args = mock_init.call_args
            config = call_args[0][0]  # First positional arg
            assert config.provider == CloudProvider.GCS
            assert config.bucket == "my-bucket"

    def test_from_uri_azure_adls(self):
        """Test Azure ADLS URI parsing."""
        uri = "abfss://container@account.dfs.core.windows.net/path"
        with patch(
            "benchbox.platforms.base.cloud_spark.staging.AzureADLSStaging.__init__",
            return_value=None,
        ) as mock_init:
            CloudSparkStaging.from_uri(uri)
            # Verify __init__ was called with correct config
            call_args = mock_init.call_args
            config = call_args[0][0]  # First positional arg
            assert config.provider == CloudProvider.AZURE_ADLS
            assert "container@account" in config.bucket

    def test_from_uri_dbfs(self):
        """Test DBFS URI parsing."""
        with patch(
            "benchbox.platforms.base.cloud_spark.staging.DBFSStaging.__init__",
            return_value=None,
        ) as mock_init:
            CloudSparkStaging.from_uri("dbfs:/Volumes/catalog/schema/volume/data")
            # Verify __init__ was called with correct config
            call_args = mock_init.call_args
            config = call_args[0][0]  # First positional arg
            assert config.provider == CloudProvider.DBFS
            assert "Volumes" in config.prefix

    def test_from_uri_local(self):
        """Test local file URI parsing."""
        staging = CloudSparkStaging.from_uri("file:///tmp/data")

        assert isinstance(staging, LocalStaging)
        assert staging.config.provider == CloudProvider.LOCAL

    def test_from_uri_unsupported_scheme(self):
        """Test unsupported URI scheme raises error."""
        with pytest.raises(ValueError, match="Unsupported URI scheme"):
            CloudSparkStaging.from_uri("hdfs://cluster/data")


class TestLocalStaging:
    """Test LocalStaging implementation."""

    def test_upload_file(self):
        """Test local file upload (copy)."""
        with tempfile.TemporaryDirectory() as tmpdir:
            source_dir = Path(tmpdir) / "source"
            staging_dir = Path(tmpdir) / "staging"
            source_dir.mkdir()

            # Create test file
            test_file = source_dir / "test.parquet"
            test_file.write_text("test data")

            # Create staging
            config = StagingConfig(
                uri=f"file://{staging_dir}",
                provider=CloudProvider.LOCAL,
                bucket="",
                prefix=str(staging_dir),
            )
            staging = LocalStaging(config)

            # Upload
            uri = staging.upload_file(test_file, "table/test.parquet")

            assert "test.parquet" in uri
            assert (staging_dir / "table" / "test.parquet").exists()

    def test_file_exists(self):
        """Test file existence check."""
        with tempfile.TemporaryDirectory() as tmpdir:
            staging_dir = Path(tmpdir)
            (staging_dir / "existing.txt").write_text("data")

            config = StagingConfig(
                uri=f"file://{staging_dir}",
                provider=CloudProvider.LOCAL,
                bucket="",
                prefix=str(staging_dir),
            )
            staging = LocalStaging(config)

            assert staging.file_exists("existing.txt")
            assert not staging.file_exists("nonexistent.txt")

    def test_list_files(self):
        """Test file listing."""
        with tempfile.TemporaryDirectory() as tmpdir:
            staging_dir = Path(tmpdir)
            table_dir = staging_dir / "lineitem"
            table_dir.mkdir()

            (table_dir / "part1.parquet").write_text("data1")
            (table_dir / "part2.parquet").write_text("data2")

            config = StagingConfig(
                uri=f"file://{staging_dir}",
                provider=CloudProvider.LOCAL,
                bucket="",
                prefix=str(staging_dir),
            )
            staging = LocalStaging(config)

            files = staging.list_files("lineitem/")
            assert len(files) == 2
            assert any("part1.parquet" in f for f in files)

    def test_delete_path(self):
        """Test file deletion."""
        with tempfile.TemporaryDirectory() as tmpdir:
            staging_dir = Path(tmpdir)
            test_file = staging_dir / "to_delete.txt"
            test_file.write_text("delete me")

            config = StagingConfig(
                uri=f"file://{staging_dir}",
                provider=CloudProvider.LOCAL,
                bucket="",
                prefix=str(staging_dir),
            )
            staging = LocalStaging(config)

            assert test_file.exists()
            staging.delete_path("to_delete.txt")
            assert not test_file.exists()

    def test_upload_tables(self):
        """Test uploading multiple tables."""
        with tempfile.TemporaryDirectory() as tmpdir:
            source_dir = Path(tmpdir) / "source"
            staging_dir = Path(tmpdir) / "staging"
            source_dir.mkdir()

            # Create test table files
            (source_dir / "lineitem.parquet").write_text("lineitem data")
            (source_dir / "orders.parquet").write_text("orders data")

            config = StagingConfig(
                uri=f"file://{staging_dir}",
                provider=CloudProvider.LOCAL,
                bucket="",
                prefix=str(staging_dir),
            )
            staging = LocalStaging(config)

            uploaded = staging.upload_tables(
                tables=["lineitem", "orders"],
                source_dir=source_dir,
                file_format="parquet",
            )

            assert "lineitem" in uploaded
            assert "orders" in uploaded
            assert (staging_dir / "lineitem").exists()
            assert (staging_dir / "orders").exists()

    def test_tables_exist(self):
        """Test checking if tables exist in staging."""
        with tempfile.TemporaryDirectory() as tmpdir:
            staging_dir = Path(tmpdir)

            # Create one table
            (staging_dir / "lineitem").mkdir()
            (staging_dir / "lineitem" / "data.parquet").write_text("data")

            config = StagingConfig(
                uri=f"file://{staging_dir}",
                provider=CloudProvider.LOCAL,
                bucket="",
                prefix=str(staging_dir),
            )
            staging = LocalStaging(config)

            assert staging.tables_exist(["lineitem"])
            assert not staging.tables_exist(["lineitem", "orders"])

    def test_get_table_uri(self):
        """Test getting table URI."""
        config = StagingConfig(
            uri="file:///tmp/staging",
            provider=CloudProvider.LOCAL,
            bucket="",
            prefix="/tmp/staging",
        )
        staging = LocalStaging(config)

        uri = staging.get_table_uri("lineitem")
        assert uri == "file:///tmp/staging/lineitem/"


class TestS3Staging:
    """Test S3Staging implementation with mocked boto3."""

    def test_s3_upload_file(self):
        """Test S3 file upload."""
        config = StagingConfig(
            uri="s3://my-bucket/data",
            provider=CloudProvider.AWS_S3,
            bucket="my-bucket",
            prefix="data",
        )
        staging = S3Staging(config)

        mock_client = MagicMock()
        staging._client = mock_client

        with tempfile.NamedTemporaryFile(suffix=".parquet") as tmp:
            tmp.write(b"test data")
            tmp.flush()

            uri = staging.upload_file(Path(tmp.name), "table/file.parquet")

            mock_client.upload_file.assert_called_once()
            assert uri == "s3://my-bucket/data/table/file.parquet"

    def test_s3_file_exists_true(self):
        """Test S3 file existence check - file exists."""
        config = StagingConfig(
            uri="s3://my-bucket/data",
            provider=CloudProvider.AWS_S3,
            bucket="my-bucket",
            prefix="data",
        )
        staging = S3Staging(config)

        mock_client = MagicMock()
        mock_client.head_object.return_value = {}
        mock_client.exceptions = MagicMock()
        staging._client = mock_client

        assert staging.file_exists("table/file.parquet")
        mock_client.head_object.assert_called_with(Bucket="my-bucket", Key="data/table/file.parquet")

    def test_s3_list_files(self):
        """Test S3 file listing."""
        config = StagingConfig(
            uri="s3://my-bucket/data",
            provider=CloudProvider.AWS_S3,
            bucket="my-bucket",
            prefix="data",
        )
        staging = S3Staging(config)

        mock_client = MagicMock()
        mock_paginator = MagicMock()
        mock_paginator.paginate.return_value = [
            {"Contents": [{"Key": "data/table/file1.parquet"}, {"Key": "data/table/file2.parquet"}]}
        ]
        mock_client.get_paginator.return_value = mock_paginator
        staging._client = mock_client

        files = staging.list_files("table/")

        assert len(files) == 2
        assert "data/table/file1.parquet" in files

    def test_s3_delete_recursive(self):
        """Test S3 recursive delete."""
        config = StagingConfig(
            uri="s3://my-bucket/data",
            provider=CloudProvider.AWS_S3,
            bucket="my-bucket",
            prefix="data",
        )
        staging = S3Staging(config)

        mock_client = MagicMock()
        mock_paginator = MagicMock()
        mock_paginator.paginate.return_value = [{"Contents": [{"Key": "data/table/file1.parquet"}]}]
        mock_client.get_paginator.return_value = mock_paginator
        staging._client = mock_client

        staging.delete_path("table/", recursive=True)

        mock_client.delete_objects.assert_called_once()


class TestStagingConfig:
    """Test StagingConfig dataclass."""

    def test_default_values(self):
        """Test default configuration values."""
        config = StagingConfig(
            uri="s3://bucket/path",
            provider=CloudProvider.AWS_S3,
            bucket="bucket",
            prefix="path",
        )

        assert config.parallel_uploads == 4
        assert config.chunk_size == 8 * 1024 * 1024
        assert config.compression is None
        assert config.region is None

    def test_custom_values(self):
        """Test custom configuration values."""
        config = StagingConfig(
            uri="s3://bucket/path",
            provider=CloudProvider.AWS_S3,
            bucket="bucket",
            prefix="path",
            region="us-west-2",
            compression="zstd",
            parallel_uploads=8,
        )

        assert config.region == "us-west-2"
        assert config.compression == "zstd"
        assert config.parallel_uploads == 8
