"""Permalink generation for published benchmark results.

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

from __future__ import annotations

import base64
import hashlib
import secrets
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any


@dataclass
class Permalink:
    """Represents a permalink to a published artifact."""

    artifact_id: str
    short_code: str
    full_url: str
    storage_path: str
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    expires_at: datetime | None = None
    access_count: int = 0
    metadata: dict[str, Any] = field(default_factory=dict)

    @property
    def is_expired(self) -> bool:
        """Check if permalink has expired."""
        if self.expires_at is None:
            return False
        return datetime.now(timezone.utc) > self.expires_at

    @property
    def days_until_expiry(self) -> int | None:
        """Get days until expiry, or None if no expiry."""
        if self.expires_at is None:
            return None
        delta = self.expires_at - datetime.now(timezone.utc)
        return max(0, delta.days)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "artifact_id": self.artifact_id,
            "short_code": self.short_code,
            "full_url": self.full_url,
            "storage_path": self.storage_path,
            "created_at": self.created_at.isoformat(),
            "expires_at": self.expires_at.isoformat() if self.expires_at else None,
            "is_expired": self.is_expired,
            "days_until_expiry": self.days_until_expiry,
            "access_count": self.access_count,
            "metadata": self.metadata,
        }


class PermalinkGenerator:
    """Generate unique permalinks for benchmark artifacts.

    Creates short, unique identifiers that can be used to reference
    published benchmark results. Supports configurable base URLs
    and expiration policies.

    Example:
        >>> generator = PermalinkGenerator(base_url="https://benchbox.io/results")
        >>> permalink = generator.generate(artifact_id="abc123", storage_path="s3://bucket/results/abc123.json")
        >>> print(permalink.full_url)
        https://benchbox.io/results/bx_a1b2c3d4
    """

    PREFIX = "bx_"
    SHORT_CODE_LENGTH = 8

    def __init__(
        self,
        base_url: str = "",
        default_expiry_days: int = 365,
    ):
        """Initialize permalink generator.

        Args:
            base_url: Base URL for permalinks (e.g., "https://benchbox.io/results").
            default_expiry_days: Default expiration in days (0 for no expiry).
        """
        self.base_url = base_url.rstrip("/")
        self.default_expiry_days = default_expiry_days
        self._generated_codes: set[str] = set()

    def generate(
        self,
        artifact_id: str,
        storage_path: str,
        expiry_days: int | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> Permalink:
        """Generate a permalink for an artifact.

        Args:
            artifact_id: Unique identifier for the artifact.
            storage_path: Storage location of the artifact.
            expiry_days: Days until expiry (None uses default).
            metadata: Additional metadata to attach.

        Returns:
            Permalink object with generated short code and URL.
        """
        short_code = self._generate_short_code(artifact_id)

        if self.base_url:
            full_url = f"{self.base_url}/{short_code}"
        else:
            full_url = short_code

        days = expiry_days if expiry_days is not None else self.default_expiry_days
        if days > 0:
            expires_at = datetime.now(timezone.utc) + timedelta(days=days)
        else:
            expires_at = None

        return Permalink(
            artifact_id=artifact_id,
            short_code=short_code,
            full_url=full_url,
            storage_path=storage_path,
            expires_at=expires_at,
            metadata=metadata or {},
        )

    def _generate_short_code(self, artifact_id: str) -> str:
        """Generate a unique short code for an artifact."""
        # Combine artifact ID with random bytes and timestamp for uniqueness
        unique_input = f"{artifact_id}:{secrets.token_hex(4)}:{time.time_ns()}"
        hash_bytes = hashlib.sha256(unique_input.encode()).digest()

        # Use URL-safe base64 encoding and take first N characters
        encoded = base64.urlsafe_b64encode(hash_bytes).decode()
        short_code = f"{self.PREFIX}{encoded[: self.SHORT_CODE_LENGTH]}"

        # Ensure uniqueness within this session
        while short_code in self._generated_codes:
            unique_input = f"{artifact_id}:{secrets.token_hex(4)}:{time.time_ns()}"
            hash_bytes = hashlib.sha256(unique_input.encode()).digest()
            encoded = base64.urlsafe_b64encode(hash_bytes).decode()
            short_code = f"{self.PREFIX}{encoded[: self.SHORT_CODE_LENGTH]}"

        self._generated_codes.add(short_code)
        return short_code

    def generate_batch(
        self,
        artifacts: list[tuple[str, str]],
        expiry_days: int | None = None,
    ) -> list[Permalink]:
        """Generate permalinks for multiple artifacts.

        Args:
            artifacts: List of (artifact_id, storage_path) tuples.
            expiry_days: Days until expiry for all permalinks.

        Returns:
            List of Permalink objects.
        """
        return [self.generate(artifact_id, storage_path, expiry_days) for artifact_id, storage_path in artifacts]


@dataclass
class PermalinkRegistry:
    """Registry for tracking generated permalinks.

    Maintains a mapping of short codes to permalinks and provides
    lookup and management capabilities.
    """

    permalinks: dict[str, Permalink] = field(default_factory=dict)

    def register(self, permalink: Permalink) -> None:
        """Register a permalink in the registry."""
        self.permalinks[permalink.short_code] = permalink

    def lookup(self, short_code: str) -> Permalink | None:
        """Look up a permalink by short code."""
        permalink = self.permalinks.get(short_code)
        if permalink and not permalink.is_expired:
            permalink.access_count += 1
            return permalink
        return None

    def lookup_by_artifact(self, artifact_id: str) -> Permalink | None:
        """Look up a permalink by artifact ID."""
        for permalink in self.permalinks.values():
            if permalink.artifact_id == artifact_id and not permalink.is_expired:
                return permalink
        return None

    def remove_expired(self) -> int:
        """Remove expired permalinks from registry.

        Returns:
            Number of permalinks removed.
        """
        expired = [code for code, link in self.permalinks.items() if link.is_expired]
        for code in expired:
            del self.permalinks[code]
        return len(expired)

    def get_active_count(self) -> int:
        """Get count of active (non-expired) permalinks."""
        return sum(1 for link in self.permalinks.values() if not link.is_expired)

    def to_dict(self) -> dict[str, Any]:
        """Convert registry to dictionary."""
        return {
            "permalinks": {code: link.to_dict() for code, link in self.permalinks.items()},
            "total_count": len(self.permalinks),
            "active_count": self.get_active_count(),
        }

    def export_index(self) -> list[dict[str, Any]]:
        """Export a simplified index of all permalinks."""
        return [
            {
                "short_code": link.short_code,
                "artifact_id": link.artifact_id,
                "full_url": link.full_url,
                "created_at": link.created_at.isoformat(),
                "is_expired": link.is_expired,
            }
            for link in self.permalinks.values()
        ]
