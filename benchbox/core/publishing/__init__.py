"""Automated data publishing pipeline for benchmark results.

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.

This module provides automated publishing of benchmark results to cloud storage
with permalink generation and artifact management.

Example usage:
    >>> from benchbox.core.publishing import Publisher, PublishingConfig
    >>> config = PublishingConfig.for_local("/path/to/published")
    >>> publisher = Publisher(config)
    >>> result = publisher.publish_result(benchmark_result)
    >>> print(result.permalink.full_url)
"""

from .artifacts import (
    Artifact,
    ArtifactManager,
    ArtifactMetadata,
    ArtifactStatus,
    ArtifactType,
)
from .config import (
    PublishFormat,
    PublishingConfig,
    RetentionPolicy,
    StorageConfig,
    StorageProvider,
)
from .permalink import (
    Permalink,
    PermalinkGenerator,
    PermalinkRegistry,
)
from .publisher import (
    PublishBatchResult,
    Publisher,
    PublishResult,
)

__all__ = [
    # Configuration
    "PublishFormat",
    "PublishingConfig",
    "RetentionPolicy",
    "StorageConfig",
    "StorageProvider",
    # Artifacts
    "Artifact",
    "ArtifactManager",
    "ArtifactMetadata",
    "ArtifactStatus",
    "ArtifactType",
    # Permalinks
    "Permalink",
    "PermalinkGenerator",
    "PermalinkRegistry",
    # Publisher
    "PublishBatchResult",
    "Publisher",
    "PublishResult",
]
