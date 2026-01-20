"""Automated data publishing pipeline.

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

from __future__ import annotations

import json
import logging
import shutil
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .artifacts import (
    Artifact,
    ArtifactManager,
    ArtifactMetadata,
    ArtifactType,
)
from .config import (
    PublishFormat,
    PublishingConfig,
    StorageProvider,
)
from .permalink import Permalink, PermalinkGenerator, PermalinkRegistry

logger = logging.getLogger(__name__)


@dataclass
class PublishResult:
    """Result of a publish operation."""

    success: bool
    artifact: Artifact | None = None
    permalink: Permalink | None = None
    published_paths: dict[str, str] = field(default_factory=dict)
    errors: list[str] = field(default_factory=list)
    published_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "success": self.success,
            "artifact_id": self.artifact.artifact_id if self.artifact else None,
            "permalink": self.permalink.to_dict() if self.permalink else None,
            "published_paths": self.published_paths,
            "errors": self.errors,
            "published_at": self.published_at.isoformat(),
        }


@dataclass
class PublishBatchResult:
    """Result of publishing multiple items."""

    total: int = 0
    successful: int = 0
    failed: int = 0
    results: list[PublishResult] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "total": self.total,
            "successful": self.successful,
            "failed": self.failed,
            "results": [r.to_dict() for r in self.results],
            "errors": self.errors,
        }


class Publisher:
    """Automated data publishing pipeline.

    Handles publishing benchmark results to cloud storage,
    generating permalinks, and managing artifacts.

    Example:
        >>> config = PublishingConfig.for_local("/path/to/published")
        >>> publisher = Publisher(config)
        >>> result = publisher.publish_result(benchmark_result)
        >>> print(result.permalink.full_url)
    """

    def __init__(self, config: PublishingConfig | None = None):
        """Initialize publisher.

        Args:
            config: Publishing configuration. Uses defaults if not provided.
        """
        self.config = config or PublishingConfig()
        self.artifact_manager = ArtifactManager()
        self.permalink_generator = PermalinkGenerator(
            base_url=self.config.permalink_base_url,
            default_expiry_days=self.config.permalink_expiry_days,
        )
        self.permalink_registry = PermalinkRegistry()

        # Ensure primary storage directory exists for local storage
        if self.config.primary_storage.provider == StorageProvider.LOCAL:
            local_path = Path(self.config.primary_storage.bucket)
            local_path.mkdir(parents=True, exist_ok=True)

    def publish_result(
        self,
        result: Any,
        name: str | None = None,
        metadata: ArtifactMetadata | None = None,
    ) -> PublishResult:
        """Publish a benchmark result.

        Args:
            result: BenchmarkResults object or dictionary to publish.
            name: Optional name for the artifact.
            metadata: Optional metadata.

        Returns:
            PublishResult with details of the operation.
        """
        errors: list[str] = []
        published_paths: dict[str, str] = {}

        # Extract metadata from result if not provided
        if metadata is None:
            metadata = self._extract_metadata(result)

        # Generate artifact name if not provided
        if name is None:
            name = self._generate_artifact_name(result, metadata)

        # Convert result to JSON content
        try:
            content = self._serialize_result(result)
        except Exception as e:
            return PublishResult(
                success=False,
                errors=[f"Failed to serialize result: {e}"],
            )

        # Create artifact
        artifact = self.artifact_manager.create_from_content(
            content=content,
            artifact_type=ArtifactType.RESULT,
            name=name,
            format="json",
            metadata=metadata,
        )

        # Publish to each format
        for publish_format in self.config.formats:
            try:
                path = self._publish_format(artifact, content, publish_format, result)
                published_paths[publish_format.value] = path
            except Exception as e:
                errors.append(f"Failed to publish {publish_format.value}: {e}")
                logger.error(f"Failed to publish {publish_format.value}: {e}")

        # Update artifact status
        if published_paths:
            self.artifact_manager.mark_published(
                artifact.artifact_id,
                published_paths.get("json", list(published_paths.values())[0]),
            )

        # Generate permalink
        permalink = None
        if self.config.generate_permalinks and published_paths:
            permalink = self.permalink_generator.generate(
                artifact_id=artifact.artifact_id,
                storage_path=published_paths.get("json", ""),
                metadata=metadata.to_dict() if metadata else {},
            )
            self.permalink_registry.register(permalink)

        success = len(published_paths) > 0 and len(errors) == 0

        return PublishResult(
            success=success,
            artifact=artifact,
            permalink=permalink,
            published_paths=published_paths,
            errors=errors,
        )

    def publish_file(
        self,
        source_path: str | Path,
        artifact_type: ArtifactType = ArtifactType.RESULT,
        name: str | None = None,
        metadata: ArtifactMetadata | None = None,
    ) -> PublishResult:
        """Publish an existing file.

        Args:
            source_path: Path to file to publish.
            artifact_type: Type of artifact.
            name: Optional name.
            metadata: Optional metadata.

        Returns:
            PublishResult with details of the operation.
        """
        path = Path(source_path)
        if not path.exists():
            return PublishResult(
                success=False,
                errors=[f"Source file not found: {source_path}"],
            )

        # Create artifact from file
        artifact = self.artifact_manager.create_from_file(
            source_path=path,
            artifact_type=artifact_type,
            name=name or path.name,
            metadata=metadata or ArtifactMetadata(),
        )

        # Copy to published location
        try:
            dest_path = self._copy_to_storage(path, artifact)
            self.artifact_manager.mark_published(artifact.artifact_id, dest_path)
        except Exception as e:
            return PublishResult(
                success=False,
                artifact=artifact,
                errors=[f"Failed to copy file: {e}"],
            )

        # Generate permalink
        permalink = None
        if self.config.generate_permalinks:
            permalink = self.permalink_generator.generate(
                artifact_id=artifact.artifact_id,
                storage_path=dest_path,
                metadata=metadata.to_dict() if metadata else {},
            )
            self.permalink_registry.register(permalink)

        return PublishResult(
            success=True,
            artifact=artifact,
            permalink=permalink,
            published_paths={artifact.format: dest_path},
        )

    def publish_batch(
        self,
        items: list[tuple[Any, ArtifactMetadata | None]],
    ) -> PublishBatchResult:
        """Publish multiple results.

        Args:
            items: List of (result, metadata) tuples.

        Returns:
            PublishBatchResult with aggregated results.
        """
        batch_result = PublishBatchResult(total=len(items))

        for result, metadata in items:
            try:
                publish_result = self.publish_result(result, metadata=metadata)
                batch_result.results.append(publish_result)

                if publish_result.success:
                    batch_result.successful += 1
                else:
                    batch_result.failed += 1
                    batch_result.errors.extend(publish_result.errors)

            except Exception as e:
                batch_result.failed += 1
                batch_result.errors.append(f"Unexpected error: {e}")

        return batch_result

    def _publish_format(
        self,
        artifact: Artifact,
        content: str,
        publish_format: PublishFormat,
        original_result: Any,
    ) -> str:
        """Publish content in a specific format."""
        # Generate filename
        base_name = artifact.name.rsplit(".", 1)[0]
        filename = f"{base_name}.{publish_format.value}"

        # Convert content if needed
        if publish_format == PublishFormat.JSON:
            output_content = content
        elif publish_format == PublishFormat.HTML:
            output_content = self._generate_html(json.loads(content), original_result)
        elif publish_format == PublishFormat.CSV:
            output_content = self._generate_csv(json.loads(content))
        else:
            output_content = content

        # Write to storage
        return self._write_to_storage(filename, output_content)

    def _write_to_storage(self, filename: str, content: str) -> str:
        """Write content to configured storage."""
        storage = self.config.primary_storage

        if storage.provider == StorageProvider.LOCAL:
            dest_path = Path(storage.bucket) / filename
            dest_path.parent.mkdir(parents=True, exist_ok=True)
            dest_path.write_text(content, encoding="utf-8")
            return str(dest_path)

        # For cloud storage, use cloudpathlib
        try:
            from benchbox.utils.cloud_storage import create_path_handler

            cloud_path = create_path_handler(f"{storage.base_uri}/{filename}")
            cloud_path.write_text(content)  # type: ignore
            return str(cloud_path)
        except ImportError:
            raise RuntimeError(
                f"Cloud storage not available. Install cloudpathlib for {storage.provider.value} support."
            )

    def _copy_to_storage(self, source_path: Path, artifact: Artifact) -> str:
        """Copy a file to configured storage."""
        storage = self.config.primary_storage
        filename = artifact.name

        if storage.provider == StorageProvider.LOCAL:
            dest_path = Path(storage.bucket) / filename
            dest_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(source_path, dest_path)
            return str(dest_path)

        # For cloud storage, use cloudpathlib
        try:
            from benchbox.utils.cloud_storage import create_path_handler

            cloud_path = create_path_handler(f"{storage.base_uri}/{filename}")
            cloud_path.write_bytes(source_path.read_bytes())  # type: ignore
            return str(cloud_path)
        except ImportError:
            raise RuntimeError(
                f"Cloud storage not available. Install cloudpathlib for {storage.provider.value} support."
            )

    def _serialize_result(self, result: Any) -> str:
        """Serialize result to JSON string."""
        if hasattr(result, "to_dict"):
            data = result.to_dict()
        elif hasattr(result, "__dict__"):
            data = result.__dict__
        elif isinstance(result, dict):
            data = result
        else:
            data = {"result": str(result)}

        # Add publishing metadata
        data["_published"] = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "anonymized": self.config.anonymize,
        }

        return json.dumps(data, indent=2, default=str)

    def _extract_metadata(self, result: Any) -> ArtifactMetadata:
        """Extract metadata from a result object or dictionary."""
        if isinstance(result, dict):
            benchmark_name = result.get("benchmark_name", "") or result.get("benchmark_id", "")
            platform = result.get("platform", "")
            scale_factor = result.get("scale_factor", 1.0)
            execution_id = result.get("execution_id", "")
        else:
            benchmark_name = getattr(result, "benchmark_name", "") or getattr(result, "benchmark_id", "")
            platform = getattr(result, "platform", "")
            scale_factor = getattr(result, "scale_factor", 1.0)
            execution_id = getattr(result, "execution_id", "")

        return ArtifactMetadata(
            benchmark_name=str(benchmark_name),
            platform=str(platform),
            scale_factor=float(scale_factor) if scale_factor else 1.0,
            execution_id=str(execution_id),
        )

    def _generate_artifact_name(self, result: Any, metadata: ArtifactMetadata) -> str:
        """Generate artifact name from result and metadata."""
        parts = []

        if metadata.benchmark_name:
            parts.append(metadata.benchmark_name.lower())
        if metadata.platform:
            parts.append(metadata.platform.lower())
        if metadata.scale_factor and metadata.scale_factor != 1.0:
            parts.append(f"sf{metadata.scale_factor}")

        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        parts.append(timestamp)

        return "_".join(parts) + ".json"

    def _generate_html(self, data: dict[str, Any], original_result: Any) -> str:
        """Generate HTML report from result data."""
        benchmark_name = data.get("benchmark_name", "Unknown")
        platform = data.get("platform", "Unknown")
        timestamp = data.get("timestamp", datetime.now(timezone.utc).isoformat())

        return f"""<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <title>BenchBox Result - {benchmark_name}</title>
    <style>
        body {{ font-family: Arial, sans-serif; margin: 40px; background: #f5f5f5; }}
        .container {{ max-width: 1000px; margin: 0 auto; background: white; padding: 30px; border-radius: 8px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); }}
        h1 {{ color: #333; border-bottom: 2px solid #4a90d9; padding-bottom: 10px; }}
        .meta {{ color: #666; margin-bottom: 20px; }}
        pre {{ background: #f8f8f8; padding: 15px; border-radius: 4px; overflow-x: auto; }}
        table {{ border-collapse: collapse; width: 100%; margin: 20px 0; }}
        th, td {{ border: 1px solid #ddd; padding: 10px; text-align: left; }}
        th {{ background: #4a90d9; color: white; }}
        .success {{ color: green; }}
        .failed {{ color: red; }}
    </style>
</head>
<body>
    <div class="container">
        <h1>{benchmark_name} Benchmark Results</h1>
        <div class="meta">
            <p><strong>Platform:</strong> {platform}</p>
            <p><strong>Timestamp:</strong> {timestamp}</p>
        </div>
        <h2>Raw Data</h2>
        <pre>{json.dumps(data, indent=2, default=str)}</pre>
        <footer>
            <p>Generated by BenchBox Publishing Pipeline</p>
        </footer>
    </div>
</body>
</html>"""

    def _generate_csv(self, data: dict[str, Any]) -> str:
        """Generate CSV from result data."""
        import csv
        import io

        output = io.StringIO()
        writer = csv.writer(output)

        # Write header
        writer.writerow(["Key", "Value"])

        # Flatten and write data
        def flatten(obj: Any, prefix: str = "") -> list[tuple[str, str]]:
            items = []
            if isinstance(obj, dict):
                for k, v in obj.items():
                    new_key = f"{prefix}.{k}" if prefix else k
                    items.extend(flatten(v, new_key))
            elif isinstance(obj, list):
                for i, v in enumerate(obj):
                    new_key = f"{prefix}[{i}]"
                    items.extend(flatten(v, new_key))
            else:
                items.append((prefix, str(obj)))
            return items

        for key, value in flatten(data):
            writer.writerow([key, value])

        return output.getvalue()

    def apply_retention(self) -> dict[str, Any]:
        """Apply retention policy to artifacts.

        Returns:
            Dictionary with retention results.
        """
        policy = self.config.retention_policy
        archived_ids = self.artifact_manager.apply_retention_policy(
            max_artifacts=policy.max_artifacts,
            max_age_days=policy.max_age_days,
            keep_latest=policy.keep_latest,
        )

        # Also clean up expired permalinks
        expired_permalinks = self.permalink_registry.remove_expired()

        return {
            "artifacts_archived": len(archived_ids),
            "archived_artifact_ids": archived_ids,
            "permalinks_expired": expired_permalinks,
        }

    def get_statistics(self) -> dict[str, Any]:
        """Get publishing statistics."""
        artifact_stats = self.artifact_manager.get_statistics()
        permalink_stats = self.permalink_registry.to_dict()

        return {
            "artifacts": artifact_stats,
            "permalinks": {
                "total": permalink_stats.get("total_count", 0),
                "active": permalink_stats.get("active_count", 0),
            },
            "storage": {
                "provider": self.config.primary_storage.provider.value,
                "base_uri": self.config.primary_storage.base_uri,
            },
        }

    def export_manifest(self) -> dict[str, Any]:
        """Export full manifest of published artifacts and permalinks."""
        return {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "config": self.config.to_dict(),
            "artifacts": self.artifact_manager.export_manifest(),
            "permalinks": self.permalink_registry.export_index(),
            "statistics": self.get_statistics(),
        }
