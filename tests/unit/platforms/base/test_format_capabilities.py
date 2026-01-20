"""Tests for format capabilities system."""

import pytest

from benchbox.platforms.base.format_capabilities import (
    DELTA_CAPABILITY,
    ICEBERG_CAPABILITY,
    PARQUET_CAPABILITY,
    SupportLevel,
    get_format_capability,
    get_preferred_format,
    get_supported_formats,
    has_feature,
    is_format_supported,
)

pytestmark = pytest.mark.fast


class TestSupportLevel:
    """Tests for SupportLevel enum."""

    def test_support_levels_exist(self):
        """Test that all support levels exist."""
        assert SupportLevel.NATIVE.value == "native"
        assert SupportLevel.EXTENSION.value == "extension"
        assert SupportLevel.EXPERIMENTAL.value == "experimental"
        assert SupportLevel.NOT_SUPPORTED.value == "not_supported"


class TestFormatCapabilities:
    """Tests for format capability definitions."""

    def test_parquet_capability(self):
        """Test Parquet capability definition."""
        assert PARQUET_CAPABILITY.format_name == "parquet"
        assert PARQUET_CAPABILITY.display_name == "Apache Parquet"
        assert PARQUET_CAPABILITY.file_extension == ".parquet"
        assert "predicate_pushdown" in PARQUET_CAPABILITY.features
        assert "column_pruning" in PARQUET_CAPABILITY.features

    def test_parquet_platform_support(self):
        """Test Parquet platform support."""
        assert PARQUET_CAPABILITY.supported_platforms["duckdb"] == SupportLevel.NATIVE
        assert PARQUET_CAPABILITY.supported_platforms["datafusion"] == SupportLevel.NATIVE
        assert PARQUET_CAPABILITY.supported_platforms["postgresql"] == SupportLevel.EXTENSION

    def test_delta_capability(self):
        """Test Delta Lake capability definition."""
        assert DELTA_CAPABILITY.format_name == "delta"
        assert DELTA_CAPABILITY.display_name == "Delta Lake"
        assert "time_travel" in DELTA_CAPABILITY.features
        assert "z_order" in DELTA_CAPABILITY.features

    def test_iceberg_capability(self):
        """Test Iceberg capability definition."""
        assert ICEBERG_CAPABILITY.format_name == "iceberg"
        assert ICEBERG_CAPABILITY.display_name == "Apache Iceberg"
        assert "partition_evolution" in ICEBERG_CAPABILITY.features
        assert "hidden_partitioning" in ICEBERG_CAPABILITY.features


class TestGetSupportedFormats:
    """Tests for get_supported_formats function."""

    def test_duckdb_supported_formats(self):
        """Test DuckDB supported formats."""
        formats = get_supported_formats("duckdb")
        assert "parquet" in formats
        assert "delta" in formats
        assert "tbl" in formats
        # Check ordering (should be by preference)
        assert formats.index("parquet") < formats.index("tbl")

    def test_datafusion_supported_formats(self):
        """Test DataFusion supported formats."""
        formats = get_supported_formats("datafusion")
        assert "parquet" in formats
        assert "tbl" in formats

    def test_databricks_supported_formats(self):
        """Test Databricks supported formats."""
        formats = get_supported_formats("databricks")
        assert "delta" in formats
        assert "parquet" in formats
        # Delta should be preferred over Parquet for Databricks
        assert formats.index("delta") < formats.index("parquet")

    def test_unknown_platform(self):
        """Test unknown platform returns empty list."""
        formats = get_supported_formats("unknown_platform")
        assert formats == []


class TestGetPreferredFormat:
    """Tests for get_preferred_format function."""

    def test_duckdb_preferred_format(self):
        """Test DuckDB preferred format."""
        # No available formats specified - returns most preferred
        preferred = get_preferred_format("duckdb")
        assert preferred == "parquet"

    def test_duckdb_with_available_formats(self):
        """Test DuckDB with specific available formats."""
        # When only tbl available
        preferred = get_preferred_format("duckdb", ["tbl", "csv"])
        assert preferred == "tbl"

        # When parquet available
        preferred = get_preferred_format("duckdb", ["tbl", "parquet"])
        assert preferred == "parquet"

    def test_databricks_preferred_format(self):
        """Test Databricks preferred format."""
        preferred = get_preferred_format("databricks")
        assert preferred == "delta"

    def test_fallback_to_tbl(self):
        """Test fallback to most preferred when no available formats specified."""
        # When no available formats specified, returns most preferred supported format
        preferred = get_preferred_format("duckdb", [])
        assert preferred == "parquet"  # DuckDB's most preferred format

    def test_fallback_to_first_available(self):
        """Test fallback to first available format."""
        # If none of the supported formats are available
        preferred = get_preferred_format("unknown_platform", ["custom_format"])
        assert preferred == "custom_format"


class TestIsFormatSupported:
    """Tests for is_format_supported function."""

    def test_parquet_support(self):
        """Test Parquet support detection."""
        assert is_format_supported("duckdb", "parquet") is True
        assert is_format_supported("datafusion", "parquet") is True
        assert is_format_supported("unknown_platform", "parquet") is False

    def test_delta_support(self):
        """Test Delta Lake support detection."""
        assert is_format_supported("databricks", "delta") is True
        assert is_format_supported("duckdb", "delta") is True
        assert is_format_supported("datafusion", "delta") is False

    def test_legacy_formats_always_supported(self):
        """Test that legacy formats (tbl, csv) are always supported."""
        assert is_format_supported("any_platform", "tbl") is True
        assert is_format_supported("any_platform", "csv") is True
        assert is_format_supported("any_platform", "dat") is True

    def test_unknown_format(self):
        """Test unknown format returns False."""
        assert is_format_supported("duckdb", "unknown_format") is False


class TestGetFormatCapability:
    """Tests for get_format_capability function."""

    def test_get_parquet_capability(self):
        """Test getting Parquet capability."""
        cap = get_format_capability("parquet")
        assert cap is not None
        assert cap.format_name == "parquet"

    def test_get_delta_capability(self):
        """Test getting Delta capability."""
        cap = get_format_capability("delta")
        assert cap is not None
        assert cap.format_name == "delta"

    def test_get_unknown_capability(self):
        """Test getting unknown capability returns None."""
        cap = get_format_capability("unknown")
        assert cap is None


class TestHasFeature:
    """Tests for has_feature function."""

    def test_parquet_features(self):
        """Test Parquet features."""
        assert has_feature("parquet", "predicate_pushdown") is True
        assert has_feature("parquet", "column_pruning") is True
        assert has_feature("parquet", "time_travel") is False

    def test_delta_features(self):
        """Test Delta Lake features."""
        assert has_feature("delta", "time_travel") is True
        assert has_feature("delta", "z_order") is True
        assert has_feature("delta", "predicate_pushdown") is False

    def test_iceberg_features(self):
        """Test Iceberg features."""
        assert has_feature("iceberg", "partition_evolution") is True
        assert has_feature("iceberg", "time_travel") is True
        assert has_feature("iceberg", "z_order") is False

    def test_unknown_format_features(self):
        """Test unknown format has no features."""
        assert has_feature("unknown", "any_feature") is False
