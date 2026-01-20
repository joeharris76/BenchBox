"""Manifest upload and pre-upload validation utilities.

Provides reusable logic to validate whether data already exists at a remote
destination according to a manifest, and whether an upload should be skipped.

Key concepts:
- UploadValidationEngine: Orchestrates remote checks and local vs remote
  manifest comparison to decide if a fresh upload is necessary.
- RemoteManifestValidator: Focused on manifest parsing, comparison, and basic
  file existence validation using a remote file system adapter.

The implementation intentionally avoids tight coupling to a particular cloud
provider by using a small RemoteFileSystemAdapter interface exposed via
benchbox.utils.cloud_storage.

Architecture:
    This module follows the same validation messaging pattern as data generation
    validation (TPC-H/TPC-DS generators):
    - print_validation_report() handles ALL validation success messages
    - should_upload_data() delegates to print_validation_report() for messaging
    - Validation messages use % formatting (lazy evaluation) not f-strings

Platform Integration Status:
    - Databricks: INTEGRATED - Uses UploadValidationEngine for UC Volume uploads
    - BigQuery: NOT APPLICABLE - Uses direct GCS upload without manifest validation
    - Snowflake: NOT APPLICABLE - Uses PUT to internal stage without manifest validation
    - Redshift: NOT APPLICABLE - Uses S3 staging with COPY command, no manifest validation

Usage Example:
    from pathlib import Path
    from benchbox.core.upload_validation import UploadValidationEngine

    # Initialize engine (optionally with custom RemoteFileSystemAdapter)
    engine = UploadValidationEngine()

    # Check if upload needed
    remote_path = "dbfs:/Volumes/catalog/schema/volume"
    local_manifest = Path("/local/data/_datagen_manifest.json")

    should_upload, result = engine.should_upload_data(
        remote_path=remote_path,
        local_manifest_path=local_manifest,
        force_upload=False,
        verbose=True  # Show detailed validation info
    )

    if not should_upload:
        # Reuse existing remote data
        remote_manifest = result.remote_manifest
        tables = remote_manifest.get("tables", {})
        # Extract file URIs from manifest
    else:
        # Upload needed - validation failed or no remote data
        if result.errors:
            print(f"Validation errors: {result.errors}")
        # Proceed with upload

Output Example (verbose=True):
    ✅ Valid TPCH data found for scale factor 1.0
    ✅ Data validation PASSED (8 tables)
       Tables: customer, lineitem, nation, orders, part, partsupp, region, supplier
       Total files: 8
       Compression: gzip (level 6)
    Skipping upload (existing data is valid)
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from benchbox.core.validation.engines import ValidationResult
from benchbox.utils.cloud_storage import (
    RemoteFileSystemAdapter,
    get_remote_fs_adapter,
)
from benchbox.utils.datagen_manifest import MANIFEST_FILENAME, get_table_files, summarise_manifest

logger = logging.getLogger(__name__)


@dataclass
class ManifestComparisonResult:
    """Result of comparing a local and remote manifest."""

    manifests_match: bool
    scale_factor_match: bool
    benchmark_match: bool
    table_count_match: bool
    file_count_match: bool
    compression_match: bool
    differences: list[str] = field(default_factory=list)


class RemoteManifestValidator:
    """Manifest-based validation against a remote file system."""

    def __init__(self, fs: RemoteFileSystemAdapter | None = None):
        self._fs = fs

    def _ensure_fs(self, remote_path: str) -> RemoteFileSystemAdapter:
        if self._fs is not None:
            return self._fs
        return get_remote_fs_adapter(remote_path)

    def load_remote_manifest(self, remote_path: str, manifest_name: str = MANIFEST_FILENAME) -> dict | None:
        """Load and parse remote manifest JSON if present."""
        fs = self._ensure_fs(remote_path)
        manifest_remote = remote_path.rstrip("/") + "/" + manifest_name
        try:
            if not fs.file_exists(manifest_remote):
                return None
            raw = fs.read_file(manifest_remote)
            return json.loads(raw.decode("utf-8"))
        except Exception as e:
            logger.debug(f"Failed to load remote manifest from {manifest_remote}: {e}")
            return None

    def compare_manifests(self, local_manifest: dict, remote_manifest: dict) -> ManifestComparisonResult:
        """Compare essential manifest fields, tolerate timestamp/version drift."""
        diffs: list[str] = []

        local_bench = str(local_manifest.get("benchmark", "")).lower()
        remote_bench = str(remote_manifest.get("benchmark", "")).lower()
        benchmark_match = local_bench == remote_bench
        if not benchmark_match:
            diffs.append(f"Benchmark mismatch: local={local_bench}, remote={remote_bench}")

        # Tolerate int/float representations
        def _to_float(v: Any) -> float | None:
            try:
                return float(v)
            except Exception:
                return None

        local_sf = _to_float(local_manifest.get("scale_factor"))
        remote_sf = _to_float(remote_manifest.get("scale_factor"))
        scale_factor_match = local_sf is not None and remote_sf is not None and local_sf == remote_sf
        if not scale_factor_match:
            diffs.append(f"Scale factor mismatch: local={local_sf}, remote={remote_sf}")

        # Compression structure may contain booleans/None; compare dicts ignoring unknown keys
        def _normalize_compression(d: Any) -> dict:
            base = {"enabled": None, "type": None, "level": None}
            if isinstance(d, dict):
                base.update({k: d.get(k) for k in base})
            return base

        comp_local = _normalize_compression(local_manifest.get("compression"))
        comp_remote = _normalize_compression(remote_manifest.get("compression"))
        compression_match = comp_local == comp_remote
        if not compression_match:
            diffs.append(f"Compression mismatch: local={comp_local}, remote={comp_remote}")

        # Table/file counts
        lc_tables, lc_files = summarise_manifest(local_manifest)
        rm_tables, rm_files = summarise_manifest(remote_manifest)

        table_count_match = lc_tables == rm_tables
        if not table_count_match:
            diffs.append(f"Table count mismatch: local={lc_tables}, remote={rm_tables}")

        file_count_match = lc_files == rm_files
        if not file_count_match:
            diffs.append(f"File count mismatch: local={lc_files}, remote={rm_files}")

        manifests_match = (
            benchmark_match and scale_factor_match and table_count_match and file_count_match and compression_match
        )

        return ManifestComparisonResult(
            manifests_match=manifests_match,
            scale_factor_match=scale_factor_match,
            benchmark_match=benchmark_match,
            table_count_match=table_count_match,
            file_count_match=file_count_match,
            compression_match=compression_match,
            differences=diffs,
        )

    def validate_remote_files_exist(self, remote_path: str, manifest: dict) -> ValidationResult:
        """Validate that all files referenced by manifest exist remotely.

        This does not compute row counts; it checks presence and non-zero sizes
        according to manifest metadata. Conservative: any missing file -> invalid.
        """
        fs = self._ensure_fs(remote_path)
        errors: list[str] = []
        warnings: list[str] = []

        tables = manifest.get("tables") or {}
        for table_name in tables.keys():
            entries = get_table_files(manifest, table_name)
            if not entries:
                warnings.append(f"No files listed for table {table_name}")
                continue
            for entry in entries:
                rel = entry.get("path")
                size = int(entry.get("size_bytes", 0))
                if not rel:
                    errors.append(f"Invalid manifest entry for {table_name}: missing path")
                    continue
                target = remote_path.rstrip("/") + "/" + str(rel)
                try:
                    exists = fs.file_exists(target)
                except Exception as e:  # pragma: no cover - provider specific runtime
                    logger.debug(f"file_exists({target}) failed: {e}")
                    exists = False

                if not exists:
                    errors.append(f"Remote file missing: {rel}")
                    continue

                # Opportunistic size check when available via stat/read
                try:
                    content = fs.read_file(target)
                    actual_size = len(content)
                    if size and actual_size != size:
                        warnings.append(f"Size mismatch for {rel}: manifest={size} bytes, remote={actual_size} bytes")
                    if actual_size == 0:
                        warnings.append(f"Remote file is empty: {rel}")
                except Exception:
                    # If read not supported or too expensive, skip size verification
                    pass

        return ValidationResult(is_valid=len(errors) == 0, errors=errors, warnings=warnings)


class UploadValidationEngine:
    """Main validation orchestrator for pre-upload checks."""

    def __init__(self, fs: RemoteFileSystemAdapter | None = None):
        self._fs = fs

    def _ensure_fs(self, remote_path: str) -> RemoteFileSystemAdapter:
        if self._fs is not None:
            return self._fs
        return get_remote_fs_adapter(remote_path)

    def check_remote_data_exists(self, remote_path: str, manifest_file: str = MANIFEST_FILENAME) -> bool:
        """Return True if a manifest exists at remote_path (indicates prior upload)."""
        fs = self._ensure_fs(remote_path)
        target = remote_path.rstrip("/") + "/" + manifest_file
        try:
            return fs.file_exists(target)
        except Exception as e:
            logger.debug(f"check_remote_data_exists failed for {target}: {e}")
            return False

    def validate_remote_data(self, remote_path: str, local_manifest_path: Path) -> ValidationResult:
        """Compare local manifest to remote and verify remote file presence."""
        if not Path(local_manifest_path).exists():
            return ValidationResult(
                is_valid=False,
                errors=[f"Local manifest not found: {local_manifest_path}"],
                warnings=[],
            )

        try:
            local_manifest = json.loads(Path(local_manifest_path).read_text(encoding="utf-8"))
        except Exception as e:
            return ValidationResult(is_valid=False, errors=[f"Failed to parse local manifest: {e}"], warnings=[])

        fs = self._ensure_fs(remote_path)
        manifest_validator = RemoteManifestValidator(fs)
        remote_manifest = manifest_validator.load_remote_manifest(remote_path)
        if not remote_manifest:
            return ValidationResult(
                is_valid=False,
                errors=["Remote manifest not found"],
                warnings=[],
                details={"local_manifest": local_manifest},
            )

        compare = manifest_validator.compare_manifests(local_manifest, remote_manifest)
        if not compare.manifests_match:
            return ValidationResult(
                is_valid=False,
                errors=["Manifest comparison failed"] + compare.differences,
                warnings=[],
                details={
                    "comparison": compare.__dict__,
                    "remote_manifest": remote_manifest,
                    "local_manifest": local_manifest,
                },
            )

        # Validate that all remote files listed by manifest exist and are non-empty
        files_validation = manifest_validator.validate_remote_files_exist(remote_path, remote_manifest)
        # Merge results
        is_valid = files_validation.is_valid
        res = ValidationResult(
            is_valid=is_valid,
            errors=list(files_validation.errors),
            warnings=list(files_validation.warnings),
            details={
                "remote_manifest": remote_manifest,
                "local_manifest": local_manifest,
                "comparison": compare.__dict__,
            },
            remote_manifest=remote_manifest,
        )
        return res

    def print_validation_report(
        self,
        validation_result: ValidationResult,
        verbose: bool = False,
    ) -> None:
        """Print user-friendly validation report for upload validation.

        Reports on successful validation of existing remote data. Follows the same
        pattern as data generation validation reporting (TPC-H/TPC-DS generators).

        Example output (non-verbose):
            ✅ Valid TPCH data found for scale factor 1.0
            ✅ Data validation PASSED (8 tables)
            Skipping upload (existing data is valid)

        Example output (verbose):
            ✅ Valid TPCH data found for scale factor 1.0
            ✅ Data validation PASSED (8 tables)
               Tables: customer, lineitem, nation, orders, part, partsupp, region, supplier
               Total files: 8
               Compression: gzip (level 6)
            Skipping upload (existing data is valid)

        Args:
            validation_result: Validation result containing remote manifest
            verbose: Show detailed validation information (table list, file counts, compression)

        Note:
            Only prints for successful validation (is_valid=True). Invalid results
            are handled by the caller via ValidationResult.errors.
        """
        if not validation_result.is_valid:
            return  # Only report on successful validation

        remote_manifest = validation_result.remote_manifest
        if not remote_manifest:
            return  # No manifest to report on

        # Extract metadata
        benchmark = str(remote_manifest.get("benchmark", "Unknown")).upper()
        scale_factor = remote_manifest.get("scale_factor", "unknown")
        tables = remote_manifest.get("tables", {}) or {}
        table_count, file_count = summarise_manifest(remote_manifest)

        # Guard against zero tables edge case
        if table_count == 0:
            logger.warning("Remote manifest has zero tables - validation passed but data may be incomplete")
            return

        # Print validation success messages using % formatting (lazy evaluation)
        logger.info("✅ Valid %s data found for scale factor %s", benchmark, scale_factor)
        logger.info("✅ Data validation PASSED (%d tables)", table_count)

        if verbose:
            # Show detailed table information
            logger.info("   Tables: %s", ", ".join(sorted(tables.keys())))
            logger.info("   Total files: %d", file_count)

            # Show compression info if available
            compression = remote_manifest.get("compression", {})
            if compression and compression.get("enabled"):
                comp_type = compression.get("type", "unknown")
                comp_level = compression.get("level", "default")
                logger.info("   Compression: %s (level %s)", comp_type, comp_level)

        # Print skip message to match data generation pattern (TPC-H generator.py:602)
        logger.info("Skipping upload (existing data is valid)")

    def should_upload_data(
        self, remote_path: str, local_manifest_path: Path, force_upload: bool = False, verbose: bool = False
    ) -> tuple[bool, ValidationResult]:
        """Decide whether a fresh upload is needed.

        Args:
            remote_path: Remote storage path (e.g., dbfs:/Volumes/...)
            local_manifest_path: Path to local manifest file
            force_upload: Force upload even if valid data exists
            verbose: Show detailed validation information

        Returns (should_upload, validation_result)
        - If force_upload is True, always return (True, ...)
        - If no remote manifest, return (True, ...)
        - If manifests match and files exist, return (False, ...)
        - Otherwise, return (True, ...)
        """
        if force_upload:
            return True, ValidationResult(is_valid=False, warnings=["Force upload requested"], errors=[])

        exists = self.check_remote_data_exists(remote_path)
        if not exists:
            return True, ValidationResult(is_valid=False, warnings=["No remote manifest found"], errors=[])

        validation = self.validate_remote_data(remote_path, local_manifest_path)
        if validation.is_valid:
            # Print validation report when data is valid and will be reused
            # (print_validation_report now includes the skip message)
            self.print_validation_report(validation, verbose=verbose)
            return False, validation
        return True, validation


__all__ = [
    "UploadValidationEngine",
    "RemoteManifestValidator",
    "ManifestComparisonResult",
]
