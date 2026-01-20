"""Shared utilities for BenchBox visualization."""

from __future__ import annotations


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
