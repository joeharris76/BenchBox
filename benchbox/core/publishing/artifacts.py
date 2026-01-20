"""Artifact management for benchmark results.

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any


class ArtifactType(str, Enum):
    """Types of benchmark artifacts."""

    RESULT = "result"
    REPORT = "report"
    COMPARISON = "comparison"
    MANIFEST = "manifest"
    DATA = "data"
    LOG = "log"


class ArtifactStatus(str, Enum):
    """Status of an artifact."""

    PENDING = "pending"
    PUBLISHED = "published"
    ARCHIVED = "archived"
    DELETED = "deleted"
    ERROR = "error"


@dataclass
class ArtifactMetadata:
    """Metadata for a benchmark artifact."""

    benchmark_name: str = ""
    platform: str = ""
    scale_factor: float = 1.0
    execution_id: str = ""
    tags: list[str] = field(default_factory=list)
    custom: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "benchmark_name": self.benchmark_name,
            "platform": self.platform,
            "scale_factor": self.scale_factor,
            "execution_id": self.execution_id,
            "tags": self.tags,
            "custom": self.custom,
        }


@dataclass
class Artifact:
    """Represents a benchmark artifact that can be published."""

    artifact_id: str
    artifact_type: ArtifactType
    name: str
    source_path: str
    content_hash: str = ""
    size_bytes: int = 0
    format: str = "json"
    status: ArtifactStatus = ArtifactStatus.PENDING
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    published_at: datetime | None = None
    published_path: str = ""
    metadata: ArtifactMetadata = field(default_factory=ArtifactMetadata)
    version: int = 1

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "artifact_id": self.artifact_id,
            "artifact_type": self.artifact_type.value,
            "name": self.name,
            "source_path": self.source_path,
            "content_hash": self.content_hash,
            "size_bytes": self.size_bytes,
            "format": self.format,
            "status": self.status.value,
            "created_at": self.created_at.isoformat(),
            "published_at": self.published_at.isoformat() if self.published_at else None,
            "published_path": self.published_path,
            "metadata": self.metadata.to_dict(),
            "version": self.version,
        }


class ArtifactManager:
    """Manage benchmark artifacts throughout their lifecycle.

    Tracks artifacts from creation through publication and archival,
    providing versioning, deduplication, and retention management.

    Example:
        >>> manager = ArtifactManager()
        >>> artifact = manager.create_from_file(
        ...     source_path="/path/to/result.json",
        ...     artifact_type=ArtifactType.RESULT,
        ...     metadata=ArtifactMetadata(benchmark_name="tpch", platform="duckdb"),
        ... )
        >>> print(artifact.artifact_id)
    """

    def __init__(self, storage_root: str | Path | None = None):
        """Initialize artifact manager.

        Args:
            storage_root: Root path for artifact storage.
        """
        self.storage_root = Path(storage_root) if storage_root else None
        self._artifacts: dict[str, Artifact] = {}
        self._hash_index: dict[str, str] = {}  # hash -> artifact_id

    def create_from_file(
        self,
        source_path: str | Path,
        artifact_type: ArtifactType,
        name: str | None = None,
        metadata: ArtifactMetadata | None = None,
    ) -> Artifact:
        """Create an artifact from a file.

        Args:
            source_path: Path to the source file.
            artifact_type: Type of artifact.
            name: Optional name (defaults to filename).
            metadata: Optional metadata.

        Returns:
            Created Artifact object.
        """
        path = Path(source_path)

        # Calculate content hash for deduplication
        content_hash = self._calculate_file_hash(path)

        # Check for existing artifact with same hash
        if content_hash in self._hash_index:
            existing_id = self._hash_index[content_hash]
            existing = self._artifacts.get(existing_id)
            if existing:
                # Return existing artifact with incremented version
                existing.version += 1
                return existing

        # Generate unique artifact ID
        artifact_id = self._generate_artifact_id(path, content_hash)

        # Get file size
        size_bytes = path.stat().st_size if path.exists() else 0

        # Determine format from extension
        format_ext = path.suffix.lstrip(".").lower() or "bin"

        artifact = Artifact(
            artifact_id=artifact_id,
            artifact_type=artifact_type,
            name=name or path.name,
            source_path=str(path),
            content_hash=content_hash,
            size_bytes=size_bytes,
            format=format_ext,
            metadata=metadata or ArtifactMetadata(),
        )

        self._artifacts[artifact_id] = artifact
        self._hash_index[content_hash] = artifact_id

        return artifact

    def create_from_content(
        self,
        content: str | bytes,
        artifact_type: ArtifactType,
        name: str,
        format: str = "json",
        metadata: ArtifactMetadata | None = None,
    ) -> Artifact:
        """Create an artifact from in-memory content.

        Args:
            content: Content as string or bytes.
            artifact_type: Type of artifact.
            name: Name for the artifact.
            format: Format extension.
            metadata: Optional metadata.

        Returns:
            Created Artifact object.
        """
        if isinstance(content, str):
            content_bytes = content.encode("utf-8")
        else:
            content_bytes = content

        content_hash = hashlib.sha256(content_bytes).hexdigest()[:16]

        # Check for existing artifact with same hash
        if content_hash in self._hash_index:
            existing_id = self._hash_index[content_hash]
            existing = self._artifacts.get(existing_id)
            if existing:
                existing.version += 1
                return existing

        artifact_id = self._generate_artifact_id(name, content_hash)

        artifact = Artifact(
            artifact_id=artifact_id,
            artifact_type=artifact_type,
            name=name,
            source_path="",  # In-memory, no source path
            content_hash=content_hash,
            size_bytes=len(content_bytes),
            format=format,
            metadata=metadata or ArtifactMetadata(),
        )

        self._artifacts[artifact_id] = artifact
        self._hash_index[content_hash] = artifact_id

        return artifact

    def get(self, artifact_id: str) -> Artifact | None:
        """Get artifact by ID."""
        return self._artifacts.get(artifact_id)

    def get_by_hash(self, content_hash: str) -> Artifact | None:
        """Get artifact by content hash."""
        artifact_id = self._hash_index.get(content_hash)
        if artifact_id:
            return self._artifacts.get(artifact_id)
        return None

    def mark_published(
        self,
        artifact_id: str,
        published_path: str,
    ) -> bool:
        """Mark an artifact as published.

        Args:
            artifact_id: ID of the artifact.
            published_path: Path where artifact was published.

        Returns:
            True if successful.
        """
        artifact = self._artifacts.get(artifact_id)
        if artifact:
            artifact.status = ArtifactStatus.PUBLISHED
            artifact.published_at = datetime.now(timezone.utc)
            artifact.published_path = published_path
            return True
        return False

    def mark_archived(self, artifact_id: str) -> bool:
        """Mark an artifact as archived."""
        artifact = self._artifacts.get(artifact_id)
        if artifact:
            artifact.status = ArtifactStatus.ARCHIVED
            return True
        return False

    def mark_error(self, artifact_id: str) -> bool:
        """Mark an artifact as having an error."""
        artifact = self._artifacts.get(artifact_id)
        if artifact:
            artifact.status = ArtifactStatus.ERROR
            return True
        return False

    def list_by_status(self, status: ArtifactStatus) -> list[Artifact]:
        """List artifacts by status."""
        return [a for a in self._artifacts.values() if a.status == status]

    def list_by_type(self, artifact_type: ArtifactType) -> list[Artifact]:
        """List artifacts by type."""
        return [a for a in self._artifacts.values() if a.artifact_type == artifact_type]

    def list_by_benchmark(self, benchmark_name: str) -> list[Artifact]:
        """List artifacts by benchmark name."""
        return [a for a in self._artifacts.values() if a.metadata.benchmark_name == benchmark_name]

    def get_statistics(self) -> dict[str, Any]:
        """Get statistics about managed artifacts."""
        by_status = {}
        by_type = {}
        total_size = 0

        for artifact in self._artifacts.values():
            status = artifact.status.value
            by_status[status] = by_status.get(status, 0) + 1

            atype = artifact.artifact_type.value
            by_type[atype] = by_type.get(atype, 0) + 1

            total_size += artifact.size_bytes

        return {
            "total_artifacts": len(self._artifacts),
            "by_status": by_status,
            "by_type": by_type,
            "total_size_bytes": total_size,
            "total_size_mb": total_size / (1024 * 1024),
        }

    def apply_retention_policy(
        self,
        max_artifacts: int = 100,
        max_age_days: int = 90,
        keep_latest: int = 10,
    ) -> list[str]:
        """Apply retention policy and mark old artifacts for deletion.

        Args:
            max_artifacts: Maximum number of artifacts to keep.
            max_age_days: Maximum age in days.
            keep_latest: Always keep this many latest artifacts.

        Returns:
            List of artifact IDs marked for archival.
        """
        archived_ids = []

        # Get artifacts sorted by creation time (newest first)
        sorted_artifacts = sorted(
            self._artifacts.values(),
            key=lambda a: a.created_at,
            reverse=True,
        )

        cutoff_time = datetime.now(timezone.utc)
        from datetime import timedelta

        cutoff_time = cutoff_time - timedelta(days=max_age_days)

        for i, artifact in enumerate(sorted_artifacts):
            # Skip if already archived or in error state
            if artifact.status in (ArtifactStatus.ARCHIVED, ArtifactStatus.ERROR):
                continue

            # Keep latest N artifacts
            if i < keep_latest:
                continue

            # Archive if over max count or too old
            if i >= max_artifacts or artifact.created_at < cutoff_time:
                artifact.status = ArtifactStatus.ARCHIVED
                archived_ids.append(artifact.artifact_id)

        return archived_ids

    def export_manifest(self) -> dict[str, Any]:
        """Export manifest of all artifacts."""
        return {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "statistics": self.get_statistics(),
            "artifacts": [a.to_dict() for a in self._artifacts.values()],
        }

    def _calculate_file_hash(self, path: Path) -> str:
        """Calculate SHA256 hash of file content."""
        if not path.exists():
            return ""

        hash_obj = hashlib.sha256()
        with open(path, "rb") as f:
            for chunk in iter(lambda: f.read(8192), b""):
                hash_obj.update(chunk)

        return hash_obj.hexdigest()[:16]

    def _generate_artifact_id(self, source: str | Path, content_hash: str) -> str:
        """Generate unique artifact ID."""
        source_str = str(source)
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
        combined = f"{source_str}:{content_hash}:{timestamp}"
        return hashlib.sha256(combined.encode()).hexdigest()[:12]
