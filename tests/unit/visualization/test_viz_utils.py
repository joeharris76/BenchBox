"""Unit tests for visualization utilities."""

from __future__ import annotations

import pytest

from benchbox.core.visualization.utils import slugify

pytestmark = pytest.mark.fast


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
