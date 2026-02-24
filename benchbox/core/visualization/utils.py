"""Shared utilities for BenchBox visualization."""

from __future__ import annotations

from typing import Any


def slugify(text: str) -> str:
    """Convert text to a URL/filename-safe slug.

    Replaces non-alphanumeric characters with hyphens, collapses multiple
    hyphens, and strips leading/trailing hyphens.

    Args:
        text: The text to slugify.

    Returns:
        A lowercase, hyphen-separated slug. Returns "untitled" if the result
        would be empty.
    """
    slug = "".join(ch if ch.isalnum() or ch in ("-", "_") else "-" for ch in text.strip().lower())
    while "--" in slug:
        slug = slug.replace("--", "-")
    return slug.strip("-") or "untitled"


def extract_chart_metadata(results: list) -> dict[str, Any]:
    """Extract chart metadata from results for subtitle display."""
    metadata: dict[str, Any] = {}
    if not results:
        return metadata

    r = results[0]
    metadata["benchmark"] = r.benchmark
    metadata["scale_factor"] = r.scale_factor

    # Extract platform version from raw result data
    raw = getattr(r, "raw", {}) or {}
    platform_block = raw.get("platform") or raw.get("platform_info") or {}
    version = platform_block.get("version")
    if version:
        # Avoid "DuckDB 1.0.0 1.0.0" when the label was already disambiguated
        if version in r.platform:
            metadata["platform_version"] = r.platform
        else:
            metadata["platform_version"] = f"{r.platform} {version}"

    # Extract tuning config
    config_block = raw.get("config") or {}
    tuning = config_block.get("tuning") or config_block.get("tuning_config")
    if tuning:
        metadata["tuning"] = tuning

    return metadata
