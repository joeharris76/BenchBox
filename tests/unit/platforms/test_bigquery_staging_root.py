"""Unit tests for BigQuery staging_root configuration.

Tests that BigQuery correctly parses staging_root from CloudStagingPath config.
"""

import pytest

pytestmark = pytest.mark.medium  # BigQuery staging tests (~2s)


class TestBigQueryStagingRoot:
    """Test BigQuery staging_root parsing for CloudStagingPath support."""

    def test_staging_root_parsing_simple(self):
        """Test parsing gs://bucket/ without path."""
        from benchbox.platforms.bigquery import BigQueryAdapter

        config = {
            "project_id": "test-project",
            "dataset_id": "test_dataset",
            "staging_root": "gs://test-bucket/",
        }

        adapter = BigQueryAdapter(**config)

        assert adapter.storage_bucket == "test-bucket"
        assert adapter.storage_prefix == "benchbox-data"  # default

    def test_staging_root_parsing_with_path(self):
        """Test parsing gs://bucket/path/ with custom path."""
        from benchbox.platforms.bigquery import BigQueryAdapter

        config = {
            "project_id": "test-project",
            "dataset_id": "test_dataset",
            "staging_root": "gs://test-bucket/custom/path/",
        }

        adapter = BigQueryAdapter(**config)

        assert adapter.storage_bucket == "test-bucket"
        assert adapter.storage_prefix == "custom/path"

    def test_staging_root_parsing_no_trailing_slash(self):
        """Test parsing gs://bucket/path without trailing slash."""
        from benchbox.platforms.bigquery import BigQueryAdapter

        config = {
            "project_id": "test-project",
            "dataset_id": "test_dataset",
            "staging_root": "gs://test-bucket/data",
        }

        adapter = BigQueryAdapter(**config)

        assert adapter.storage_bucket == "test-bucket"
        assert adapter.storage_prefix == "data"

    def test_staging_root_overrides_explicit_bucket(self):
        """Test that staging_root takes precedence over explicit storage_bucket."""
        from benchbox.platforms.bigquery import BigQueryAdapter

        config = {
            "project_id": "test-project",
            "dataset_id": "test_dataset",
            "storage_bucket": "old-bucket",  # Should be ignored
            "storage_prefix": "old-prefix",  # Should be ignored
            "staging_root": "gs://new-bucket/new-prefix",
        }

        adapter = BigQueryAdapter(**config)

        # staging_root takes precedence
        assert adapter.storage_bucket == "new-bucket"
        assert adapter.storage_prefix == "new-prefix"

    def test_no_staging_root_uses_explicit_config(self):
        """Test fallback to explicit storage_bucket when no staging_root."""
        from benchbox.platforms.bigquery import BigQueryAdapter

        config = {
            "project_id": "test-project",
            "dataset_id": "test_dataset",
            "storage_bucket": "explicit-bucket",
            "storage_prefix": "explicit-prefix",
        }

        adapter = BigQueryAdapter(**config)

        assert adapter.storage_bucket == "explicit-bucket"
        assert adapter.storage_prefix == "explicit-prefix"

    def test_staging_root_invalid_provider_raises_error(self):
        """Test that non-GCS staging_root raises ValueError."""
        from benchbox.platforms.bigquery import BigQueryAdapter

        config = {
            "project_id": "test-project",
            "dataset_id": "test_dataset",
            "staging_root": "s3://aws-bucket/path",  # Wrong provider!
        }

        with pytest.raises(ValueError, match="BigQuery requires GCS"):
            BigQueryAdapter(**config)

    def test_staging_root_empty_bucket(self):
        """Test parsing gs:// with minimal bucket name."""
        from benchbox.platforms.bigquery import BigQueryAdapter

        config = {
            "project_id": "test-project",
            "dataset_id": "test_dataset",
            "staging_root": "gs://b",  # Single character bucket
        }

        adapter = BigQueryAdapter(**config)

        assert adapter.storage_bucket == "b"
        assert adapter.storage_prefix == "benchbox-data"
