"""Utility helpers for writing benchmark data generation manifests.

This module centralises creation of the ``_datagen_manifest.json`` file that
describes the files produced during benchmark data generation. The manifest is
used by the CLI to decide when existing datasets can be safely reused without
triggering another expensive generation step.

The helpers here intentionally avoid expensive filesystem work – they rely on
metadata gathered during generation (row counts, file sizes) and normalise
paths so the manifest is portable across local and cloud storage backends.
"""

from __future__ import annotations

import json
from collections.abc import Iterable
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Union

import benchbox
from benchbox.utils.cloud_storage import DatabricksPath, create_path_handler

try:  # Optional cloudpathlib dependency for cloud path support
    from cloudpathlib import CloudPath  # type: ignore
except ImportError:  # pragma: no cover - dependency optional
    CloudPath = None  # type: ignore


PathLike = Union[str, Path, "CloudPath"]

MANIFEST_FILENAME = "_datagen_manifest.json"


def _utc_now_iso() -> str:
    """Return a RFC3339/ISO-8601 timestamp with UTC timezone."""

    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


@dataclass
class ManifestTableEntry:
    """Single file entry stored inside the manifest for a table."""

    path: str
    size_bytes: int
    row_count: int
    checksum: str | None = None
    format: str | None = None  # Format name (e.g., 'tbl', 'parquet', 'delta')
    metadata: dict[str, Any] | None = None  # Format-specific metadata

    def to_json(self) -> dict[str, Any]:
        """Render the entry as a JSON-serialisable mapping."""

        data = asdict(self)
        # Drop keys with None to keep manifest compact
        return {k: v for k, v in data.items() if v is not None}


def _ensure_path(path: PathLike) -> Path | CloudPath | DatabricksPath:
    """Normalise a path/URI to a ``Path`` or ``CloudPath`` instance."""

    if isinstance(path, (Path,)):
        return path
    if CloudPath is not None and isinstance(path, CloudPath):  # type: ignore
        return path
    return create_path_handler(path)


def _normalise_to_root(root: Path | CloudPath, path: PathLike) -> tuple[Path | CloudPath | DatabricksPath, str]:
    """Return the resolved path object and the manifest-relative string."""

    resolved = _ensure_path(path)

    # For relative local paths ensure we join them with the root directory first
    if isinstance(resolved, Path) and not resolved.is_absolute():
        resolved = (root / resolved).resolve()

    if CloudPath is not None and isinstance(resolved, CloudPath):  # type: ignore
        # ``relative_to`` is supported when both share the same anchor. Nested
        # try/except keeps compatibility across providers.
        try:
            rel = resolved.relative_to(root)
            return resolved, rel.as_posix()
        except Exception:  # pragma: no cover - provider specific edge cases
            return resolved, resolved.path

    try:
        rel_path = resolved.relative_to(root)
        return resolved, rel_path.as_posix()
    except Exception:
        # Fall back to absolute POSIX string when the path is outside the root
        if hasattr(resolved, "as_posix"):
            return resolved, resolved.as_posix()
        return resolved, str(resolved)


def resolve_compression_metadata(source: Any) -> dict[str, Any]:
    """Derive compression metadata for the manifest from a generator instance."""

    enabled = False
    compression_type = None
    compression_level = None

    if hasattr(source, "should_use_compression"):
        try:
            enabled = bool(source.should_use_compression())  # type: ignore[attr-defined]
        except Exception:  # pragma: no cover - defensive fallback
            enabled = False

    if enabled:
        compression_type = getattr(source, "compression_type", None)
        compression_level = getattr(source, "compression_level", None)
    else:
        compression_type = None
        compression_level = None

    return {
        "enabled": enabled,
        "type": compression_type,
        "level": compression_level,
    }


class DataGenerationManifest:
    """Helper used by generators to write ``_datagen_manifest.json`` files.

    Manifest Schema v2 (with multi-format support):
    - formats: list[str] - List of available formats (e.g., ['tbl', 'parquet', 'delta'])
    - tables: dict[table_name, dict[format, list[entries]]] - Nested structure by format

    Manifest Schema v1 (legacy, for backward compatibility):
    - tables: dict[table_name, list[entries]] - Simple list of entries per table
    """

    def __init__(
        self,
        *,
        output_dir: PathLike,
        benchmark: str,
        scale_factor: float,
        compression: dict[str, Any] | None = None,
        parallel: int | None = None,
        seed: int | None = None,
        extra_metadata: dict[str, Any] | None = None,
        formats: list[str] | None = None,
    ) -> None:
        self._root = _ensure_path(output_dir)
        self._benchmark = benchmark
        self._scale_factor = scale_factor
        self._compression = compression or {"enabled": False, "type": None, "level": None}
        self._parallel = int(parallel) if parallel not in (None, 0) else 1
        self._seed = int(seed) if seed is not None else None
        self._extra_metadata = extra_metadata or {}
        self._formats = formats or ["tbl"]  # Track which formats are generated
        # Store tables as dict[table_name, dict[format, list[entries]]]
        self._tables: dict[str, dict[str, list[ManifestTableEntry]]] = {}

    @property
    def manifest_path(self) -> Path | CloudPath:
        """Return the target manifest path (without creating it)."""

        return self._root / MANIFEST_FILENAME  # type: ignore[operator]

    def add_entry(
        self,
        table_name: str,
        file_path: PathLike,
        *,
        row_count: int,
        size_bytes: int | None = None,
        checksum: str | None = None,
        format: str = "tbl",
        metadata: dict[str, Any] | None = None,
    ) -> None:
        """Register a generated file entry for the manifest.

        Args:
            table_name: Name of the table
            file_path: Path to the data file
            row_count: Number of rows in the file
            size_bytes: Size of the file in bytes (auto-detected if None)
            checksum: Optional checksum for data validation
            format: Format name (e.g., 'tbl', 'parquet', 'delta')
            metadata: Format-specific metadata
        """

        resolved, manifest_path = _normalise_to_root(self._root, file_path)

        size = size_bytes
        if size is None and hasattr(resolved, "stat"):
            try:
                size = int(resolved.stat().st_size)  # type: ignore[union-attr]
            except Exception:  # pragma: no cover - stat may fail on some providers
                size = 0

        entry = ManifestTableEntry(
            path=manifest_path,
            size_bytes=int(size or 0),
            row_count=int(row_count),
            checksum=checksum,
            format=format,
            metadata=metadata,
        )

        # Ensure table and format exist in nested structure
        if table_name not in self._tables:
            self._tables[table_name] = {}
        if format not in self._tables[table_name]:
            self._tables[table_name][format] = []

        self._tables[table_name][format].append(entry)

        # Add format to formats list if not already there
        if format not in self._formats:
            self._formats.append(format)

    def extend(self, table_name: str, entries: Iterable[ManifestTableEntry], format: str = "tbl") -> None:
        """Add a collection of manifest entries already in canonical form.

        Args:
            table_name: Name of the table
            entries: Iterable of ManifestTableEntry objects
            format: Format name for these entries
        """

        if table_name not in self._tables:
            self._tables[table_name] = {}
        if format not in self._tables[table_name]:
            self._tables[table_name][format] = []

        self._tables[table_name][format].extend(entries)

        # Add format to formats list if not already there
        if format not in self._formats:
            self._formats.append(format)

    def file_counts(self) -> tuple[int, int]:
        """Return (table_count, file_count) for summary messaging."""

        table_count = len(self._tables)
        file_count = sum(
            len(entries) for format_entries in self._tables.values() for entries in format_entries.values()
        )
        return table_count, file_count

    def to_dict(self) -> dict[str, Any]:
        """Render the manifest content as a dictionary.

        Generates manifest in v2 format with multi-format support.
        """

        # Build tables structure: dict[table, {"formats": {format: [entries]}}]
        tables_data = {}
        for table_name, format_entries in self._tables.items():
            tables_data[table_name] = {
                "formats": {
                    format_name: [entry.to_json() for entry in entries]
                    for format_name, entries in format_entries.items()
                }
            }

        manifest: dict[str, Any] = {
            "version": 2,
            "benchmark": self._benchmark.lower(),
            "scale_factor": float(self._scale_factor),
            "formats": list(self._formats),  # List of available formats
            "format_preference": list(self._formats),  # Default preference order
            "compression": self._compression,
            "parallel": self._parallel,
            "created_at": _utc_now_iso(),
            "generator_version": benchbox.__version__,
            "tables": tables_data,
        }

        if self._seed is not None:
            manifest["seed"] = self._seed

        if self._extra_metadata:
            manifest.update(self._extra_metadata)

        return manifest

    def write(self) -> Path | CloudPath:
        """Persist the manifest JSON to disk/cloud storage."""

        manifest = self.to_dict()
        target = self.manifest_path

        # Ensure parent directory exists for local paths; cloud providers handle lazily
        if isinstance(target, Path):
            target.parent.mkdir(parents=True, exist_ok=True)

        with target.open("w", encoding="utf-8") as fh:  # type: ignore[union-attr]
            json.dump(manifest, fh, indent=2, sort_keys=False)
            fh.write("\n")

        return target


def summarise_manifest(manifest: dict[str, Any]) -> tuple[int, int]:
    """Return (table_count, file_count) for a parsed manifest dictionary.

    Handles both v1 (legacy) and v2 (multi-format) manifests.
    """

    tables = manifest.get("tables", {}) or {}
    table_count = len(tables)

    # Check manifest version (prefer explicit version, fall back to legacy key)
    version = int(manifest.get("version") or manifest.get("manifest_version", 1))

    if version == 1:
        # V1: tables = dict[table, list[entries]]
        file_count = sum(len(entries or []) for entries in tables.values())
    else:
        # V2: tables = dict[table, {"formats": {format: [entries]}}]
        file_count = 0
        for table_data in tables.values():
            formats = table_data.get("formats", {}) if isinstance(table_data, dict) else {}
            for entries in formats.values():
                file_count += len(entries or [])

    return table_count, file_count


def load_manifest(manifest_path: Path | CloudPath) -> dict[str, Any]:
    """Load and parse a manifest file.

    Args:
        manifest_path: Path to the manifest JSON file

    Returns:
        Parsed manifest dictionary

    Raises:
        FileNotFoundError: If manifest file doesn't exist
        json.JSONDecodeError: If manifest is not valid JSON
    """
    with open(manifest_path, encoding="utf-8") as f:
        return json.load(f)


def get_table_files(manifest: dict[str, Any], table_name: str, format: str | None = None) -> list[dict[str, Any]]:
    """Get file entries for a table from manifest.

    Args:
        manifest: Parsed manifest dictionary
        table_name: Name of the table
        format: Optional format to filter by (e.g., 'tbl', 'parquet').
               If None, returns files from all formats (v1) or default format (v2).

    Returns:
        List of file entry dictionaries
    """
    tables = manifest.get("tables", {})
    if table_name not in tables:
        return []

    table_data = tables[table_name]
    version = int(manifest.get("version") or manifest.get("manifest_version", 1))

    if version == 1:
        # V1: tables = dict[table, list[entries]]
        # All entries are assumed to be 'tbl' format
        return table_data if isinstance(table_data, list) else []
    else:
        # V2: tables = dict[table, {"formats": {format: [entries]}}]
        if not isinstance(table_data, dict):
            return []

        formats_dict = table_data.get("formats", {}) if isinstance(table_data, dict) else {}

        if format:
            # Return entries for specific format
            return formats_dict.get(format, [])

        # Choose default format: manifest format_preference > manifest formats list > first available
        preferred_order = manifest.get("format_preference") or manifest.get("formats") or []
        if preferred_order:
            for fmt in preferred_order:
                if fmt in formats_dict and formats_dict[fmt]:
                    return formats_dict[fmt]

        # Fallback to any available format
        for entries in formats_dict.values():
            if entries:
                return entries

        return []


def get_available_formats(manifest: dict[str, Any], table_name: str | None = None) -> list[str]:
    """Get list of available formats from manifest.

    Args:
        manifest: Parsed manifest dictionary
        table_name: Optional table name to get formats for specific table.
                   If None, returns formats at manifest level.

    Returns:
        List of format names
    """
    version = int(manifest.get("version") or manifest.get("manifest_version", 1))

    if version == 1:
        # V1 manifests only have 'tbl' format
        return ["tbl"]

    if table_name:
        # Get formats for specific table
        tables = manifest.get("tables", {})
        table_entry = tables.get(table_name)
        if isinstance(table_entry, dict):
            formats_section = table_entry.get("formats")
            if isinstance(formats_section, dict):
                return list(formats_section.keys())
        return []

    # Return manifest-level formats
    formats = manifest.get("formats") or []
    return list(formats) if formats else ["tbl"]
