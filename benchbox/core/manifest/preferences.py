"""Format preference resolution for multi-format manifests."""

from __future__ import annotations

from benchbox.core.manifest.models import ManifestV2
from benchbox.platforms.base.format_capabilities import get_supported_formats


def get_preferred_format(
    manifest: ManifestV2,
    table_name: str,
    platform_name: str,
) -> str | None:
    """Get preferred format for a table on a platform.

    Resolution order:
    1. Manifest format_preference (user-specified)
    2. Platform default preferences
    3. First available format

    Args:
        manifest: ManifestV2 instance
        table_name: Name of the table
        platform_name: Name of the platform (e.g., 'duckdb', 'datafusion')

    Returns:
        Preferred format name if available, None if no formats found
    """
    table_formats = manifest.tables.get(table_name)
    if not table_formats:
        return None

    available_formats = list(table_formats.formats.keys())
    if not available_formats:
        return None

    # Get platform supported formats (in preference order)
    platform_formats = get_supported_formats(platform_name)

    # Try manifest preference first
    for fmt in manifest.format_preference:
        if fmt in available_formats and fmt in platform_formats:
            return fmt

    # Try platform preference
    for fmt in platform_formats:
        if fmt in available_formats:
            return fmt

    # Fallback: first available format
    return available_formats[0]


def get_files_for_format(
    manifest: ManifestV2,
    table_name: str,
    format_name: str,
) -> list[str]:
    """Get file paths for a specific format.

    Args:
        manifest: ManifestV2 instance
        table_name: Name of the table
        format_name: Format to retrieve (e.g., 'parquet', 'delta')

    Returns:
        List of file paths for the specified format, empty list if not found
    """
    table_formats = manifest.tables.get(table_name)
    if not table_formats:
        return []

    files = table_formats.formats.get(format_name, [])
    return [f.path for f in files]


def list_available_formats(manifest: ManifestV2, table_name: str) -> list[str]:
    """List all available formats for a table.

    Args:
        manifest: ManifestV2 instance
        table_name: Name of the table

    Returns:
        List of format names available for the table
    """
    table_formats = manifest.tables.get(table_name)
    if not table_formats:
        return []

    return list(table_formats.formats.keys())
