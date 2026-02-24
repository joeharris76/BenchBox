"""Unit tests for visualization utilities."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from benchbox.core.visualization.utils import extract_chart_metadata, slugify

pytestmark = pytest.mark.fast


def _make_result(platform: str, version: str | None = None, benchmark: str = "tpch", sf: float = 1.0):
    """Build a minimal NormalizedResult-like object for metadata extraction tests."""
    raw: dict = {}
    if version is not None:
        raw["platform"] = {"name": platform, "version": version}
    return SimpleNamespace(platform=platform, benchmark=benchmark, scale_factor=sf, raw=raw)


class TestExtractChartMetadata:
    def test_empty_results_returns_empty_dict(self):
        assert extract_chart_metadata([]) == {}

    def test_basic_fields_extracted(self):
        r = _make_result("DuckDB", version="1.4.3")
        meta = extract_chart_metadata([r])
        assert meta["benchmark"] == "tpch"
        assert meta["scale_factor"] == 1.0

    def test_version_appended_to_platform_name(self):
        """When version is not in platform label, it gets appended."""
        r = _make_result("DuckDB", version="1.4.3")
        meta = extract_chart_metadata([r])
        assert meta["platform_version"] == "DuckDB 1.4.3"

    def test_no_duplicate_when_version_already_in_label(self):
        """After version disambiguation, platform label already contains version — no duplication."""
        r = _make_result("DuckDB 1.4.3", version="1.4.3")
        meta = extract_chart_metadata([r])
        assert meta["platform_version"] == "DuckDB 1.4.3"
        assert "DuckDB 1.4.3 1.4.3" not in meta.get("platform_version", "")

    def test_no_platform_version_when_no_raw_version(self):
        """No raw version → no platform_version key."""
        r = _make_result("DuckDB", version=None)
        meta = extract_chart_metadata([r])
        assert "platform_version" not in meta

    def test_uses_first_result_for_metadata(self):
        """Metadata is extracted from results[0]."""
        r1 = _make_result("DuckDB", version="1.0.0", benchmark="tpch", sf=1.0)
        r2 = _make_result("Polars", version="0.19.0", benchmark="tpcds", sf=10.0)
        meta = extract_chart_metadata([r1, r2])
        assert meta["benchmark"] == "tpch"
        assert "DuckDB" in meta.get("platform_version", "")


class TestSlugify:
    def test_basic_text(self):
        assert slugify("Hello World") == "hello-world"

    def test_special_characters(self):
        assert slugify("foo@bar#baz") == "foo-bar-baz"

    def test_multiple_spaces(self):
        assert slugify("foo   bar") == "foo-bar"

    def test_leading_trailing_spaces(self):
        assert slugify("  hello  ") == "hello"

    def test_preserves_hyphens(self):
        assert slugify("hello-world") == "hello-world"

    def test_preserves_underscores(self):
        assert slugify("hello_world") == "hello_world"

    def test_collapses_multiple_hyphens(self):
        assert slugify("foo--bar---baz") == "foo-bar-baz"

    def test_empty_string_returns_untitled(self):
        assert slugify("") == "untitled"

    def test_only_special_chars_returns_untitled(self):
        assert slugify("@#$%") == "untitled"

    def test_mixed_case(self):
        assert slugify("HelloWorld") == "helloworld"

    def test_numbers_preserved(self):
        assert slugify("test123") == "test123"
        assert slugify("123test") == "123test"

    def test_unicode_characters(self):
        # Unicode chars (é) are kept as-is since isalnum() returns True for them
        assert slugify("café") == "café"

    def test_platform_names(self):
        assert slugify("DuckDB") == "duckdb"
        # Parentheses become hyphens, multiple hyphens collapsed, trailing stripped
        assert slugify("Snowflake (Enterprise)") == "snowflake-enterprise"
        assert slugify("BigQuery") == "bigquery"
