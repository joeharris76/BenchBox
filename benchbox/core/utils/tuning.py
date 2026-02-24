"""Helpers for extracting benchmark tuning values."""

from __future__ import annotations

from typing import Any


def extract_constraint_flags(tuning_config: Any) -> tuple[bool, bool]:
    """Extract primary/foreign key enablement flags from a tuning configuration."""
    if tuning_config is None:
        return False, False

    return bool(tuning_config.primary_keys.enabled), bool(tuning_config.foreign_keys.enabled)
