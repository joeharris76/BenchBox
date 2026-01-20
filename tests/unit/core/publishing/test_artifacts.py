"""Tests for artifact management module.

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

import json

import pytest

from benchbox.core.publishing.artifacts import (
    Artifact,
    ArtifactManager,
    ArtifactMetadata,
    ArtifactStatus,
    ArtifactType,
)

pytestmark = pytest.mark.fast


class TestArtifactType:
    """Tests for ArtifactType enum."""

    def test_all_types(self):
        """Should have expected types."""
        types = list(ArtifactType)
        assert ArtifactType.RESULT in types
        assert ArtifactType.REPORT in types
        assert ArtifactType.COMPARISON in types
        assert ArtifactType.MANIFEST in types
        assert ArtifactType.DATA in types
        assert ArtifactType.LOG in types


class TestArtifactStatus:
    """Tests for ArtifactStatus enum."""

    def test_all_statuses(self):
        """Should have expected statuses."""
        statuses = list(ArtifactStatus)
        assert ArtifactStatus.PENDING in statuses
        assert ArtifactStatus.PUBLISHED in statuses
        assert ArtifactStatus.ARCHIVED in statuses
        assert ArtifactStatus.DELETED in statuses
        assert ArtifactStatus.ERROR in statuses


class TestArtifactMetadata:
    """Tests for ArtifactMetadata dataclass."""

    def test_defaults(self):
        """Should have sensible defaults."""
        metadata = ArtifactMetadata()
        assert metadata.benchmark_name == ""
        assert metadata.platform == ""
        assert metadata.scale_factor == 1.0
        assert metadata.tags == []

    def test_custom_values(self):
        """Should accept custom values."""
        metadata = ArtifactMetadata(
            benchmark_name="tpch",
            platform="duckdb",
            scale_factor=10.0,
            execution_id="exec123",
            tags=["production", "validated"],
            custom={"env": "staging"},
        )
        assert metadata.benchmark_name == "tpch"
        assert metadata.platform == "duckdb"
        assert metadata.scale_factor == 10.0
        assert "production" in metadata.tags

    def test_to_dict(self):
        """Should convert to dictionary."""
        metadata = ArtifactMetadata(
            benchmark_name="tpch",
            platform="duckdb",
        )
        d = metadata.to_dict()
        assert d["benchmark_name"] == "tpch"
        assert d["platform"] == "duckdb"
        assert "tags" in d
        assert "custom" in d


class TestArtifact:
    """Tests for Artifact dataclass."""

    def test_basic_creation(self):
        """Should create artifact."""
        artifact = Artifact(
            artifact_id="abc123",
            artifact_type=ArtifactType.RESULT,
            name="result.json",
            source_path="/path/to/result.json",
        )
        assert artifact.artifact_id == "abc123"
        assert artifact.artifact_type == ArtifactType.RESULT
        assert artifact.name == "result.json"
        assert artifact.status == ArtifactStatus.PENDING

    def test_defaults(self):
        """Should have sensible defaults."""
        artifact = Artifact(
            artifact_id="abc123",
            artifact_type=ArtifactType.RESULT,
            name="result.json",
            source_path="",
        )
        assert artifact.content_hash == ""
        assert artifact.size_bytes == 0
        assert artifact.format == "json"
        assert artifact.version == 1

    def test_to_dict(self):
        """Should convert to dictionary."""
        artifact = Artifact(
            artifact_id="abc123",
            artifact_type=ArtifactType.RESULT,
            name="result.json",
            source_path="/path/to/file.json",
            content_hash="abcd1234",
            size_bytes=1024,
        )
        d = artifact.to_dict()
        assert d["artifact_id"] == "abc123"
        assert d["artifact_type"] == "result"
        assert d["name"] == "result.json"
        assert d["content_hash"] == "abcd1234"
        assert d["size_bytes"] == 1024
        assert d["status"] == "pending"
        assert "created_at" in d


class TestArtifactManager:
    """Tests for ArtifactManager class."""

    @pytest.fixture
    def manager(self):
        """Create artifact manager."""
        return ArtifactManager()

    @pytest.fixture
    def sample_file(self, tmp_path):
        """Create sample file."""
        file_path = tmp_path / "result.json"
        file_path.write_text(json.dumps({"test": "data"}))
        return file_path

    def test_create_from_file(self, manager, sample_file):
        """Should create artifact from file."""
        artifact = manager.create_from_file(
            source_path=sample_file,
            artifact_type=ArtifactType.RESULT,
        )
        assert artifact.name == "result.json"
        assert artifact.artifact_type == ArtifactType.RESULT
        assert artifact.content_hash != ""
        assert artifact.size_bytes > 0
        assert artifact.format == "json"

    def test_create_from_file_with_metadata(self, manager, sample_file):
        """Should create artifact with metadata."""
        metadata = ArtifactMetadata(
            benchmark_name="tpch",
            platform="duckdb",
        )
        artifact = manager.create_from_file(
            source_path=sample_file,
            artifact_type=ArtifactType.RESULT,
            metadata=metadata,
        )
        assert artifact.metadata.benchmark_name == "tpch"
        assert artifact.metadata.platform == "duckdb"

    def test_create_from_file_custom_name(self, manager, sample_file):
        """Should accept custom name."""
        artifact = manager.create_from_file(
            source_path=sample_file,
            artifact_type=ArtifactType.RESULT,
            name="custom_name.json",
        )
        assert artifact.name == "custom_name.json"

    def test_create_from_content(self, manager):
        """Should create artifact from content."""
        content = json.dumps({"test": "data"})
        artifact = manager.create_from_content(
            content=content,
            artifact_type=ArtifactType.RESULT,
            name="test.json",
        )
        assert artifact.name == "test.json"
        assert artifact.content_hash != ""
        assert artifact.size_bytes == len(content.encode())
        assert artifact.source_path == ""

    def test_create_from_bytes(self, manager):
        """Should create artifact from bytes."""
        content = b"binary data"
        artifact = manager.create_from_content(
            content=content,
            artifact_type=ArtifactType.DATA,
            name="data.bin",
            format="bin",
        )
        assert artifact.size_bytes == len(content)
        assert artifact.format == "bin"

    def test_get_artifact(self, manager, sample_file):
        """Should get artifact by ID."""
        artifact = manager.create_from_file(
            source_path=sample_file,
            artifact_type=ArtifactType.RESULT,
        )
        found = manager.get(artifact.artifact_id)
        assert found is artifact

    def test_get_artifact_not_found(self, manager):
        """Should return None for unknown ID."""
        found = manager.get("unknown_id")
        assert found is None

    def test_get_by_hash(self, manager, sample_file):
        """Should get artifact by content hash."""
        artifact = manager.create_from_file(
            source_path=sample_file,
            artifact_type=ArtifactType.RESULT,
        )
        found = manager.get_by_hash(artifact.content_hash)
        assert found is artifact

    def test_deduplication(self, manager, tmp_path):
        """Should deduplicate identical content."""
        file1 = tmp_path / "file1.json"
        file1.write_text(json.dumps({"test": "same"}))

        file2 = tmp_path / "file2.json"
        file2.write_text(json.dumps({"test": "same"}))

        artifact1 = manager.create_from_file(file1, ArtifactType.RESULT)
        version_after_first = artifact1.version

        artifact2 = manager.create_from_file(file2, ArtifactType.RESULT)

        # Same artifact should be returned with incremented version
        assert artifact1.artifact_id == artifact2.artifact_id
        # Both point to same object, so version is incremented on the same object
        assert artifact2.version == version_after_first + 1

    def test_mark_published(self, manager, sample_file):
        """Should mark artifact as published."""
        artifact = manager.create_from_file(sample_file, ArtifactType.RESULT)
        result = manager.mark_published(
            artifact.artifact_id,
            published_path="s3://bucket/file.json",
        )
        assert result is True
        assert artifact.status == ArtifactStatus.PUBLISHED
        assert artifact.published_at is not None
        assert artifact.published_path == "s3://bucket/file.json"

    def test_mark_published_not_found(self, manager):
        """Should return False for unknown artifact."""
        result = manager.mark_published("unknown", "s3://bucket/file.json")
        assert result is False

    def test_mark_archived(self, manager, sample_file):
        """Should mark artifact as archived."""
        artifact = manager.create_from_file(sample_file, ArtifactType.RESULT)
        result = manager.mark_archived(artifact.artifact_id)
        assert result is True
        assert artifact.status == ArtifactStatus.ARCHIVED

    def test_mark_error(self, manager, sample_file):
        """Should mark artifact as error."""
        artifact = manager.create_from_file(sample_file, ArtifactType.RESULT)
        result = manager.mark_error(artifact.artifact_id)
        assert result is True
        assert artifact.status == ArtifactStatus.ERROR

    def test_list_by_status(self, manager, tmp_path):
        """Should list artifacts by status."""
        file1 = tmp_path / "file1.json"
        file1.write_text("1")
        file2 = tmp_path / "file2.json"
        file2.write_text("2")

        art1 = manager.create_from_file(file1, ArtifactType.RESULT)
        manager.create_from_file(file2, ArtifactType.RESULT)

        manager.mark_published(art1.artifact_id, "path1")

        pending = manager.list_by_status(ArtifactStatus.PENDING)
        published = manager.list_by_status(ArtifactStatus.PUBLISHED)

        assert len(pending) == 1
        assert len(published) == 1

    def test_list_by_type(self, manager, tmp_path):
        """Should list artifacts by type."""
        file1 = tmp_path / "result.json"
        file1.write_text("1")
        file2 = tmp_path / "report.html"
        file2.write_text("2")

        manager.create_from_file(file1, ArtifactType.RESULT)
        manager.create_from_file(file2, ArtifactType.REPORT)

        results = manager.list_by_type(ArtifactType.RESULT)
        reports = manager.list_by_type(ArtifactType.REPORT)

        assert len(results) == 1
        assert len(reports) == 1

    def test_list_by_benchmark(self, manager, tmp_path):
        """Should list artifacts by benchmark name."""
        file1 = tmp_path / "tpch.json"
        file1.write_text("1")
        file2 = tmp_path / "tpcds.json"
        file2.write_text("2")

        manager.create_from_file(
            file1,
            ArtifactType.RESULT,
            metadata=ArtifactMetadata(benchmark_name="tpch"),
        )
        manager.create_from_file(
            file2,
            ArtifactType.RESULT,
            metadata=ArtifactMetadata(benchmark_name="tpcds"),
        )

        tpch = manager.list_by_benchmark("tpch")
        assert len(tpch) == 1
        assert tpch[0].metadata.benchmark_name == "tpch"

    def test_get_statistics(self, manager, tmp_path):
        """Should get statistics."""
        for i in range(3):
            file_path = tmp_path / f"file{i}.json"
            file_path.write_text(json.dumps({"i": i}))
            manager.create_from_file(file_path, ArtifactType.RESULT)

        stats = manager.get_statistics()
        assert stats["total_artifacts"] == 3
        assert stats["by_status"]["pending"] == 3
        assert stats["by_type"]["result"] == 3
        assert stats["total_size_bytes"] > 0

    def test_apply_retention_policy(self, manager, tmp_path):
        """Should apply retention policy."""
        # Create 15 artifacts
        for i in range(15):
            file_path = tmp_path / f"file{i}.json"
            file_path.write_text(json.dumps({"i": i}))
            manager.create_from_file(file_path, ArtifactType.RESULT)

        # Apply policy keeping only 10
        archived = manager.apply_retention_policy(
            max_artifacts=10,
            max_age_days=90,
            keep_latest=5,
        )

        assert len(archived) == 5
        pending = manager.list_by_status(ArtifactStatus.PENDING)
        assert len(pending) == 10

    def test_export_manifest(self, manager, tmp_path):
        """Should export manifest."""
        file_path = tmp_path / "test.json"
        file_path.write_text("test")
        manager.create_from_file(file_path, ArtifactType.RESULT)

        manifest = manager.export_manifest()
        assert "generated_at" in manifest
        assert "statistics" in manifest
        assert "artifacts" in manifest
        assert len(manifest["artifacts"]) == 1
