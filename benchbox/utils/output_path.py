"""Utilities for normalizing output paths for data generation.

Ensures remote and local output roots append the dataset suffix
<benchmark>_<sf> consistently (e.g., tpch_sf01), matching local defaults.
"""

from __future__ import annotations

from .scale_factor import format_scale_factor

REMOTE_SCHEMES = ("s3://", "gs://", "abfss://", "dbfs:/")


def _ensure_suffix(root: str, suffix: str) -> str:
    r = root.rstrip("/")
    last = r.split("/")[-1] if r else ""
    if last.lower() == suffix.lower():
        return r
    return f"{r}/{suffix}" if r else suffix


def normalize_output_root(output_root: str | None, benchmark: str, scale: float) -> str | None:
    """Append <benchmark>_<sf> to output root if not present.

    Works for local paths and remote URIs (dbfs:/, s3://, gs://, abfss://).
    Idempotent: if the suffix is already present, returns the original root.
    """
    if not output_root:
        return output_root

    bench = (benchmark or "").strip().lower()
    sf = format_scale_factor(scale)
    suffix = f"{bench}_{sf}" if bench else sf
    return _ensure_suffix(output_root, suffix)
