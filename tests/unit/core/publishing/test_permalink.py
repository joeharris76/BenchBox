"""Tests for permalink generation module.

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

from datetime import datetime, timedelta, timezone

import pytest

from benchbox.core.publishing.permalink import (
    Permalink,
    PermalinkGenerator,
    PermalinkRegistry,
)

pytestmark = pytest.mark.fast


class TestPermalink:
    """Tests for Permalink dataclass."""

    def test_basic_creation(self):
        """Should create permalink."""
        permalink = Permalink(
            artifact_id="abc123",
            short_code="bx_12345678",
            full_url="https://example.com/bx_12345678",
            storage_path="s3://bucket/results/abc123.json",
        )
        assert permalink.artifact_id == "abc123"
        assert permalink.short_code == "bx_12345678"
        assert permalink.full_url == "https://example.com/bx_12345678"

    def test_not_expired_by_default(self):
        """Should not be expired if no expiry set."""
        permalink = Permalink(
            artifact_id="abc123",
            short_code="bx_12345678",
            full_url="https://example.com/bx_12345678",
            storage_path="",
            expires_at=None,
        )
        assert permalink.is_expired is False
        assert permalink.days_until_expiry is None

    def test_expired(self):
        """Should detect expired permalink."""
        past = datetime.now(timezone.utc) - timedelta(days=1)
        permalink = Permalink(
            artifact_id="abc123",
            short_code="bx_12345678",
            full_url="",
            storage_path="",
            expires_at=past,
        )
        assert permalink.is_expired is True
        assert permalink.days_until_expiry == 0

    def test_not_yet_expired(self):
        """Should detect non-expired permalink."""
        future = datetime.now(timezone.utc) + timedelta(days=30)
        permalink = Permalink(
            artifact_id="abc123",
            short_code="bx_12345678",
            full_url="",
            storage_path="",
            expires_at=future,
        )
        assert permalink.is_expired is False
        assert permalink.days_until_expiry is not None
        assert permalink.days_until_expiry >= 29

    def test_to_dict(self):
        """Should convert to dictionary."""
        permalink = Permalink(
            artifact_id="abc123",
            short_code="bx_12345678",
            full_url="https://example.com/bx_12345678",
            storage_path="s3://bucket/file.json",
            metadata={"key": "value"},
        )
        d = permalink.to_dict()
        assert d["artifact_id"] == "abc123"
        assert d["short_code"] == "bx_12345678"
        assert d["full_url"] == "https://example.com/bx_12345678"
        assert "is_expired" in d
        assert "days_until_expiry" in d
        assert d["metadata"]["key"] == "value"


class TestPermalinkGenerator:
    """Tests for PermalinkGenerator class."""

    def test_basic_generation(self):
        """Should generate permalink."""
        generator = PermalinkGenerator()
        permalink = generator.generate(
            artifact_id="abc123",
            storage_path="s3://bucket/results/abc123.json",
        )
        assert permalink.artifact_id == "abc123"
        assert permalink.short_code.startswith("bx_")
        assert len(permalink.short_code) == len("bx_") + 8

    def test_generation_with_base_url(self):
        """Should include base URL in full URL."""
        generator = PermalinkGenerator(base_url="https://benchbox.io/results")
        permalink = generator.generate(
            artifact_id="abc123",
            storage_path="s3://bucket/file.json",
        )
        assert permalink.full_url.startswith("https://benchbox.io/results/")
        assert "bx_" in permalink.full_url

    def test_generation_without_base_url(self):
        """Should use short code as full URL without base URL."""
        generator = PermalinkGenerator(base_url="")
        permalink = generator.generate(
            artifact_id="abc123",
            storage_path="s3://bucket/file.json",
        )
        assert permalink.full_url == permalink.short_code

    def test_custom_expiry_days(self):
        """Should apply custom expiry."""
        generator = PermalinkGenerator(default_expiry_days=30)
        permalink = generator.generate(
            artifact_id="abc123",
            storage_path="",
            expiry_days=7,
        )
        assert permalink.expires_at is not None
        assert permalink.days_until_expiry is not None
        assert permalink.days_until_expiry <= 7

    def test_default_expiry_days(self):
        """Should use default expiry."""
        generator = PermalinkGenerator(default_expiry_days=90)
        permalink = generator.generate(
            artifact_id="abc123",
            storage_path="",
        )
        assert permalink.expires_at is not None
        assert permalink.days_until_expiry is not None
        assert permalink.days_until_expiry <= 90

    def test_no_expiry(self):
        """Should support no expiry."""
        generator = PermalinkGenerator(default_expiry_days=0)
        permalink = generator.generate(
            artifact_id="abc123",
            storage_path="",
        )
        assert permalink.expires_at is None
        assert permalink.is_expired is False

    def test_metadata_attachment(self):
        """Should attach metadata."""
        generator = PermalinkGenerator()
        permalink = generator.generate(
            artifact_id="abc123",
            storage_path="",
            metadata={"benchmark": "tpch", "platform": "duckdb"},
        )
        assert permalink.metadata["benchmark"] == "tpch"
        assert permalink.metadata["platform"] == "duckdb"

    def test_unique_codes(self):
        """Should generate unique short codes."""
        generator = PermalinkGenerator()
        codes = set()
        for i in range(100):
            permalink = generator.generate(
                artifact_id=f"artifact_{i}",
                storage_path="",
            )
            codes.add(permalink.short_code)
        assert len(codes) == 100  # All unique

    def test_generate_batch(self):
        """Should generate batch of permalinks."""
        generator = PermalinkGenerator()
        artifacts = [
            ("artifact_1", "s3://bucket/1.json"),
            ("artifact_2", "s3://bucket/2.json"),
            ("artifact_3", "s3://bucket/3.json"),
        ]
        permalinks = generator.generate_batch(artifacts)
        assert len(permalinks) == 3
        assert all(p.short_code.startswith("bx_") for p in permalinks)

    def test_batch_with_expiry(self):
        """Should apply expiry to batch."""
        generator = PermalinkGenerator()
        artifacts = [("a", "s3://1"), ("b", "s3://2")]
        permalinks = generator.generate_batch(artifacts, expiry_days=7)
        assert all(p.days_until_expiry <= 7 for p in permalinks)


class TestPermalinkRegistry:
    """Tests for PermalinkRegistry class."""

    @pytest.fixture
    def registry(self):
        """Create empty registry."""
        return PermalinkRegistry()

    @pytest.fixture
    def sample_permalink(self):
        """Create sample permalink."""
        return Permalink(
            artifact_id="abc123",
            short_code="bx_testcode",
            full_url="https://example.com/bx_testcode",
            storage_path="s3://bucket/file.json",
        )

    def test_register(self, registry, sample_permalink):
        """Should register permalink."""
        registry.register(sample_permalink)
        assert len(registry.permalinks) == 1

    def test_lookup_by_code(self, registry, sample_permalink):
        """Should lookup by short code."""
        registry.register(sample_permalink)
        found = registry.lookup("bx_testcode")
        assert found is not None
        assert found.artifact_id == "abc123"

    def test_lookup_not_found(self, registry):
        """Should return None for unknown code."""
        found = registry.lookup("bx_unknown")
        assert found is None

    def test_lookup_increments_access_count(self, registry, sample_permalink):
        """Should increment access count on lookup."""
        registry.register(sample_permalink)
        assert sample_permalink.access_count == 0

        registry.lookup("bx_testcode")
        assert sample_permalink.access_count == 1

        registry.lookup("bx_testcode")
        assert sample_permalink.access_count == 2

    def test_lookup_expired(self, registry):
        """Should return None for expired permalink."""
        expired = Permalink(
            artifact_id="abc123",
            short_code="bx_expired",
            full_url="",
            storage_path="",
            expires_at=datetime.now(timezone.utc) - timedelta(days=1),
        )
        registry.register(expired)
        found = registry.lookup("bx_expired")
        assert found is None

    def test_lookup_by_artifact(self, registry, sample_permalink):
        """Should lookup by artifact ID."""
        registry.register(sample_permalink)
        found = registry.lookup_by_artifact("abc123")
        assert found is not None
        assert found.short_code == "bx_testcode"

    def test_lookup_by_artifact_not_found(self, registry):
        """Should return None for unknown artifact."""
        found = registry.lookup_by_artifact("unknown")
        assert found is None

    def test_remove_expired(self, registry):
        """Should remove expired permalinks."""
        active = Permalink(
            artifact_id="active",
            short_code="bx_active",
            full_url="",
            storage_path="",
            expires_at=datetime.now(timezone.utc) + timedelta(days=30),
        )
        expired = Permalink(
            artifact_id="expired",
            short_code="bx_expired",
            full_url="",
            storage_path="",
            expires_at=datetime.now(timezone.utc) - timedelta(days=1),
        )
        registry.register(active)
        registry.register(expired)

        removed = registry.remove_expired()
        assert removed == 1
        assert "bx_active" in registry.permalinks
        assert "bx_expired" not in registry.permalinks

    def test_get_active_count(self, registry):
        """Should count active permalinks."""
        active = Permalink(
            artifact_id="active",
            short_code="bx_active",
            full_url="",
            storage_path="",
        )
        expired = Permalink(
            artifact_id="expired",
            short_code="bx_expired",
            full_url="",
            storage_path="",
            expires_at=datetime.now(timezone.utc) - timedelta(days=1),
        )
        registry.register(active)
        registry.register(expired)

        assert registry.get_active_count() == 1

    def test_to_dict(self, registry, sample_permalink):
        """Should convert to dictionary."""
        registry.register(sample_permalink)
        d = registry.to_dict()
        assert "permalinks" in d
        assert "total_count" in d
        assert "active_count" in d
        assert d["total_count"] == 1

    def test_export_index(self, registry, sample_permalink):
        """Should export index."""
        registry.register(sample_permalink)
        index = registry.export_index()
        assert len(index) == 1
        assert index[0]["short_code"] == "bx_testcode"
        assert index[0]["artifact_id"] == "abc123"
