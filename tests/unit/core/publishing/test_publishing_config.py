"""Tests for publishing configuration module.

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

import pytest

from benchbox.core.publishing.config import (
    PublishFormat,
    PublishingConfig,
    RetentionPolicy,
    StorageConfig,
    StorageProvider,
)

pytestmark = pytest.mark.fast


class TestStorageProvider:
    """Tests for StorageProvider enum."""

    def test_all_providers(self):
        """Should have expected providers."""
        providers = list(StorageProvider)
        assert StorageProvider.LOCAL in providers
        assert StorageProvider.S3 in providers
        assert StorageProvider.GCS in providers
        assert StorageProvider.AZURE in providers
        assert StorageProvider.DATABRICKS in providers

    def test_string_values(self):
        """Should have string values."""
        assert StorageProvider.LOCAL.value == "local"
        assert StorageProvider.S3.value == "s3"
        assert StorageProvider.GCS.value == "gcs"


class TestPublishFormat:
    """Tests for PublishFormat enum."""

    def test_all_formats(self):
        """Should have expected formats."""
        formats = list(PublishFormat)
        assert PublishFormat.JSON in formats
        assert PublishFormat.CSV in formats
        assert PublishFormat.PARQUET in formats
        assert PublishFormat.HTML in formats


class TestStorageConfig:
    """Tests for StorageConfig dataclass."""

    def test_local_config(self):
        """Should create local storage config."""
        config = StorageConfig(
            provider=StorageProvider.LOCAL,
            bucket="/path/to/storage",
        )
        assert config.provider == StorageProvider.LOCAL
        assert config.base_uri == "/path/to/storage"

    def test_s3_config(self):
        """Should create S3 storage config."""
        config = StorageConfig(
            provider=StorageProvider.S3,
            bucket="my-bucket",
            prefix="benchbox",
            region="us-west-2",
        )
        assert config.base_uri == "s3://my-bucket/benchbox"

    def test_gcs_config(self):
        """Should create GCS storage config."""
        config = StorageConfig(
            provider=StorageProvider.GCS,
            bucket="my-bucket",
            prefix="results",
        )
        assert config.base_uri == "gs://my-bucket/results"

    def test_azure_config(self):
        """Should create Azure storage config."""
        config = StorageConfig(
            provider=StorageProvider.AZURE,
            bucket="container",
            prefix="benchbox",
        )
        assert config.base_uri == "abfss://container/benchbox"

    def test_databricks_config(self):
        """Should create Databricks storage config."""
        config = StorageConfig(
            provider=StorageProvider.DATABRICKS,
            bucket="catalog/schema/volume",
            prefix="data",
        )
        assert config.base_uri == "dbfs:/Volumes/catalog/schema/volume/data"

    def test_to_dict(self):
        """Should convert to dictionary."""
        config = StorageConfig(
            provider=StorageProvider.S3,
            bucket="my-bucket",
            prefix="results",
            region="us-east-1",
        )
        d = config.to_dict()
        assert d["provider"] == "s3"
        assert d["bucket"] == "my-bucket"
        assert d["prefix"] == "results"
        assert d["region"] == "us-east-1"
        assert "base_uri" in d

    def test_credentials_not_in_dict(self):
        """Should not include credentials in to_dict."""
        config = StorageConfig(
            provider=StorageProvider.S3,
            bucket="my-bucket",
            credentials={"access_key": "secret"},
        )
        d = config.to_dict()
        assert "credentials" not in d


class TestRetentionPolicy:
    """Tests for RetentionPolicy dataclass."""

    def test_defaults(self):
        """Should have sensible defaults."""
        policy = RetentionPolicy()
        assert policy.max_artifacts == 100
        assert policy.max_age_days == 90
        assert policy.keep_latest == 10
        assert policy.archive_after_days == 30

    def test_custom_policy(self):
        """Should accept custom values."""
        policy = RetentionPolicy(
            max_artifacts=50,
            max_age_days=30,
            keep_latest=5,
            archive_after_days=14,
        )
        assert policy.max_artifacts == 50
        assert policy.max_age_days == 30

    def test_to_dict(self):
        """Should convert to dictionary."""
        policy = RetentionPolicy()
        d = policy.to_dict()
        assert "max_artifacts" in d
        assert "max_age_days" in d
        assert "keep_latest" in d


class TestPublishingConfig:
    """Tests for PublishingConfig dataclass."""

    def test_defaults(self):
        """Should have sensible defaults."""
        config = PublishingConfig()
        assert config.primary_storage.provider == StorageProvider.LOCAL
        assert len(config.formats) >= 1
        assert config.generate_permalinks is True
        assert config.anonymize is True

    def test_for_s3(self):
        """Should create S3 configuration."""
        config = PublishingConfig.for_s3(
            bucket="my-bucket",
            prefix="results",
            region="us-west-2",
        )
        assert config.primary_storage.provider == StorageProvider.S3
        assert config.primary_storage.bucket == "my-bucket"
        assert config.primary_storage.region == "us-west-2"

    def test_for_gcs(self):
        """Should create GCS configuration."""
        config = PublishingConfig.for_gcs(
            bucket="my-bucket",
            prefix="results",
        )
        assert config.primary_storage.provider == StorageProvider.GCS
        assert config.primary_storage.bucket == "my-bucket"

    def test_for_local(self):
        """Should create local configuration."""
        config = PublishingConfig.for_local("/path/to/results")
        assert config.primary_storage.provider == StorageProvider.LOCAL
        assert config.primary_storage.bucket == "/path/to/results"

    def test_secondary_storage(self):
        """Should support secondary storage destinations."""
        config = PublishingConfig(
            secondary_storage=[
                StorageConfig(provider=StorageProvider.S3, bucket="backup-bucket"),
            ]
        )
        assert len(config.secondary_storage) == 1

    def test_custom_formats(self):
        """Should accept custom formats."""
        config = PublishingConfig(formats=[PublishFormat.JSON, PublishFormat.CSV, PublishFormat.HTML])
        assert len(config.formats) == 3

    def test_permalink_settings(self):
        """Should have permalink settings."""
        config = PublishingConfig(
            generate_permalinks=True,
            permalink_base_url="https://benchbox.io/results",
            permalink_expiry_days=30,
        )
        assert config.permalink_base_url == "https://benchbox.io/results"
        assert config.permalink_expiry_days == 30

    def test_to_dict(self):
        """Should convert to dictionary."""
        config = PublishingConfig.for_local("/tmp/test")
        d = config.to_dict()
        assert "primary_storage" in d
        assert "formats" in d
        assert "retention_policy" in d
        assert "generate_permalinks" in d
