"""Publishing pipeline configuration.

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class StorageProvider(str, Enum):
    """Supported cloud storage providers."""

    LOCAL = "local"
    S3 = "s3"
    GCS = "gcs"
    AZURE = "azure"
    DATABRICKS = "databricks"


class PublishFormat(str, Enum):
    """Formats for published artifacts."""

    JSON = "json"
    CSV = "csv"
    PARQUET = "parquet"
    HTML = "html"


@dataclass
class StorageConfig:
    """Configuration for a storage destination."""

    provider: StorageProvider
    bucket: str = ""
    prefix: str = ""
    region: str = ""
    credentials: dict[str, str] = field(default_factory=dict)

    @property
    def base_uri(self) -> str:
        """Get the base URI for this storage configuration."""
        if self.provider == StorageProvider.LOCAL:
            return self.bucket  # Local path
        elif self.provider == StorageProvider.S3:
            return f"s3://{self.bucket}/{self.prefix}".rstrip("/")
        elif self.provider == StorageProvider.GCS:
            return f"gs://{self.bucket}/{self.prefix}".rstrip("/")
        elif self.provider == StorageProvider.AZURE:
            return f"abfss://{self.bucket}/{self.prefix}".rstrip("/")
        elif self.provider == StorageProvider.DATABRICKS:
            return f"dbfs:/Volumes/{self.bucket}/{self.prefix}".rstrip("/")
        return ""

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary (excluding credentials)."""
        return {
            "provider": self.provider.value,
            "bucket": self.bucket,
            "prefix": self.prefix,
            "region": self.region,
            "base_uri": self.base_uri,
        }


@dataclass
class RetentionPolicy:
    """Policy for artifact retention."""

    max_artifacts: int = 100
    max_age_days: int = 90
    keep_latest: int = 10
    archive_after_days: int = 30

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "max_artifacts": self.max_artifacts,
            "max_age_days": self.max_age_days,
            "keep_latest": self.keep_latest,
            "archive_after_days": self.archive_after_days,
        }


@dataclass
class PublishingConfig:
    """Configuration for the publishing pipeline."""

    # Primary storage destination
    primary_storage: StorageConfig = field(
        default_factory=lambda: StorageConfig(
            provider=StorageProvider.LOCAL,
            bucket="benchmark_runs/published",
        )
    )

    # Optional secondary/mirror destinations
    secondary_storage: list[StorageConfig] = field(default_factory=list)

    # Output formats
    formats: list[PublishFormat] = field(default_factory=lambda: [PublishFormat.JSON, PublishFormat.HTML])

    # Permalink settings
    generate_permalinks: bool = True
    permalink_base_url: str = ""
    permalink_expiry_days: int = 365

    # Artifact management
    retention_policy: RetentionPolicy = field(default_factory=RetentionPolicy)

    # Metadata
    include_system_info: bool = True
    include_timestamps: bool = True
    anonymize: bool = True

    # Notifications (future extension)
    notify_on_publish: bool = False
    notification_webhooks: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "primary_storage": self.primary_storage.to_dict(),
            "secondary_storage": [s.to_dict() for s in self.secondary_storage],
            "formats": [f.value for f in self.formats],
            "generate_permalinks": self.generate_permalinks,
            "permalink_base_url": self.permalink_base_url,
            "permalink_expiry_days": self.permalink_expiry_days,
            "retention_policy": self.retention_policy.to_dict(),
            "include_system_info": self.include_system_info,
            "include_timestamps": self.include_timestamps,
            "anonymize": self.anonymize,
            "notify_on_publish": self.notify_on_publish,
        }

    @classmethod
    def for_s3(
        cls,
        bucket: str,
        prefix: str = "benchbox",
        region: str = "us-east-1",
    ) -> PublishingConfig:
        """Create configuration for S3 publishing."""
        return cls(
            primary_storage=StorageConfig(
                provider=StorageProvider.S3,
                bucket=bucket,
                prefix=prefix,
                region=region,
            )
        )

    @classmethod
    def for_gcs(
        cls,
        bucket: str,
        prefix: str = "benchbox",
    ) -> PublishingConfig:
        """Create configuration for GCS publishing."""
        return cls(
            primary_storage=StorageConfig(
                provider=StorageProvider.GCS,
                bucket=bucket,
                prefix=prefix,
            )
        )

    @classmethod
    def for_local(
        cls,
        path: str = "benchmark_runs/published",
    ) -> PublishingConfig:
        """Create configuration for local publishing."""
        return cls(
            primary_storage=StorageConfig(
                provider=StorageProvider.LOCAL,
                bucket=path,
            )
        )
