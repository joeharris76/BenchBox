"""Tests for benchbox.core.platform_config_utils module."""

import pytest

from benchbox.core.platform_config_utils import (
    extract_compute_info,
    extract_region_info,
    get_platform_summary,
    normalize_warehouse_size,
    standardize_cloud_provider,
)


class TestNormalizeWarehouseSize:
    def test_none_size_returns_none(self):
        assert normalize_warehouse_size("snowflake", None) is None

    def test_empty_size_returns_none(self):
        assert normalize_warehouse_size("snowflake", "") is None

    def test_databricks_known_sizes(self):
        result = normalize_warehouse_size("databricks", "2X-Small")
        assert result["tier"] == "xxs"
        assert result["raw_value"] == "2X-Small"
        assert result["platform"] == "databricks"

        result = normalize_warehouse_size("databricks", "X-Small")
        assert result["tier"] == "xs"

        result = normalize_warehouse_size("databricks", "Large")
        assert result["tier"] == "l"

    def test_databricks_unknown_size(self):
        result = normalize_warehouse_size("databricks", "mega")
        assert result["tier"] == "custom"

    def test_snowflake_known_sizes(self):
        result = normalize_warehouse_size("snowflake", "X-SMALL")
        assert result["tier"] == "xs"
        assert result["raw_value"] == "X-SMALL"
        assert result["platform"] == "snowflake"

        result = normalize_warehouse_size("snowflake", "medium")
        assert result["tier"] == "m"

    def test_snowflake_unknown_size(self):
        result = normalize_warehouse_size("snowflake", "7x-large")
        assert result["tier"] == "custom"

    def test_redshift_node_types(self):
        result = normalize_warehouse_size("redshift", "dc2.large")
        assert result["tier"] == "small"
        assert result["node_type"] == "dc2.large"

        result = normalize_warehouse_size("redshift", "ra3.16xlarge")
        assert result["tier"] == "xlarge"

    def test_redshift_unknown_node(self):
        result = normalize_warehouse_size("redshift", "ds2.xlarge")
        assert result["tier"] == "custom"

    def test_bigquery_slot_counts(self):
        result = normalize_warehouse_size("bigquery", "50")
        assert result["tier"] == "small"
        assert result["slot_count"] == 50

        result = normalize_warehouse_size("bigquery", "500")
        assert result["tier"] == "large"

        result = normalize_warehouse_size("bigquery", "5000")
        assert result["tier"] == "xlarge"

    def test_bigquery_non_numeric(self):
        result = normalize_warehouse_size("bigquery", "on-demand")
        assert result["tier"] == "on-demand"

    def test_unknown_platform(self):
        result = normalize_warehouse_size("duckdb", "default")
        assert result["tier"] == "n/a"
        assert result["raw_value"] == "default"
        assert result["platform"] == "duckdb"


class TestStandardizeCloudProvider:
    def test_none_returns_none(self):
        assert standardize_cloud_provider(None) is None

    def test_empty_returns_none(self):
        assert standardize_cloud_provider("") is None

    def test_aws_aliases(self):
        assert standardize_cloud_provider("aws") == "AWS"
        assert standardize_cloud_provider("amazon") == "AWS"
        assert standardize_cloud_provider("amazon web services") == "AWS"

    def test_gcp_aliases(self):
        assert standardize_cloud_provider("gcp") == "GCP"
        assert standardize_cloud_provider("google") == "GCP"
        assert standardize_cloud_provider("google cloud platform") == "GCP"

    def test_azure_aliases(self):
        assert standardize_cloud_provider("azure") == "Azure"
        assert standardize_cloud_provider("microsoft azure") == "Azure"

    def test_case_insensitive(self):
        assert standardize_cloud_provider("AWS") == "AWS"
        assert standardize_cloud_provider("GCP") == "GCP"
        assert standardize_cloud_provider("AZURE") == "Azure"

    def test_unknown_provider_passthrough(self):
        assert standardize_cloud_provider("OVH") == "OVH"

    def test_whitespace_handling(self):
        assert standardize_cloud_provider("  aws  ") == "AWS"


class TestExtractRegionInfo:
    def test_direct_fields(self):
        platform_info = {"cloud_provider": "aws", "cloud_region": "us-east-1"}
        result = extract_region_info(platform_info)
        assert result["cloud_provider"] == "AWS"
        assert result["cloud_region"] == "us-east-1"

    def test_nested_configuration(self):
        platform_info = {
            "configuration": {"cloud_provider": "gcp", "region": "us-central1"},
        }
        result = extract_region_info(platform_info)
        assert result["cloud_provider"] == "GCP"
        assert result["cloud_region"] == "us-central1"

    def test_direct_overrides_nested(self):
        platform_info = {
            "cloud_provider": "aws",
            "cloud_region": "eu-west-1",
            "configuration": {"cloud_provider": "gcp", "region": "us-central1"},
        }
        result = extract_region_info(platform_info)
        assert result["cloud_provider"] == "AWS"
        assert result["cloud_region"] == "eu-west-1"

    def test_empty_returns_nones(self):
        result = extract_region_info({})
        assert result["cloud_provider"] is None
        assert result["cloud_region"] is None


class TestExtractComputeInfo:
    def test_warehouse_size(self):
        platform_info = {
            "platform_type": "snowflake",
            "compute_configuration": {"warehouse_size": "LARGE"},
        }
        result = extract_compute_info(platform_info)
        assert result["raw_size"] == "LARGE"
        assert result["normalized_size"]["tier"] == "l"

    def test_node_count(self):
        platform_info = {
            "platform_type": "redshift",
            "compute_configuration": {"node_type": "dc2.large", "num_compute_nodes": 4},
        }
        result = extract_compute_info(platform_info)
        assert result["node_count"] == 4

    def test_cluster_range(self):
        platform_info = {
            "platform_type": "snowflake",
            "compute_configuration": {
                "warehouse_size": "LARGE",
                "min_num_clusters": 1,
                "max_num_clusters": 3,
            },
        }
        result = extract_compute_info(platform_info)
        assert result["min_clusters"] == 1
        assert result["max_clusters"] == 3

    def test_databricks_features(self):
        platform_info = {
            "platform_type": "databricks",
            "compute_configuration": {
                "cluster_size": "small",
                "enable_photon": True,
                "enable_serverless_compute": False,
            },
        }
        result = extract_compute_info(platform_info)
        assert result["photon_enabled"] is True
        assert result["serverless_enabled"] is False

    def test_snowflake_query_acceleration(self):
        platform_info = {
            "platform_type": "snowflake",
            "compute_configuration": {
                "warehouse_size": "MEDIUM",
                "enable_query_acceleration": True,
            },
        }
        result = extract_compute_info(platform_info)
        assert result["query_acceleration"] is True

    def test_bigquery_pricing_model(self):
        platform_info = {
            "platform_type": "bigquery",
            "compute_configuration": {"pricing_model": "on-demand"},
        }
        result = extract_compute_info(platform_info)
        assert result["pricing_model"] == "on-demand"

    def test_empty_compute_config(self):
        result = extract_compute_info({"platform_type": "duckdb"})
        assert result == {}


class TestGetPlatformSummary:
    def test_basic_summary(self):
        platform_info = {"platform_name": "DuckDB"}
        result = get_platform_summary(platform_info)
        assert "DuckDB" in result

    def test_with_version(self):
        platform_info = {"platform_name": "DuckDB", "platform_version": "0.10.0"}
        result = get_platform_summary(platform_info)
        assert "DuckDB 0.10.0" in result

    def test_with_cloud_and_region(self):
        platform_info = {
            "platform_name": "Snowflake",
            "platform_version": "8.2.1",
            "cloud_provider": "AWS",
            "cloud_region": "us-east-1",
            "compute_configuration": {"warehouse_size": "LARGE"},
        }
        result = get_platform_summary(platform_info)
        assert "Snowflake 8.2.1" in result
        assert "on AWS (us-east-1)" in result
        assert "warehouse: LARGE" in result

    def test_unknown_platform(self):
        result = get_platform_summary({})
        assert "Unknown Platform" in result
