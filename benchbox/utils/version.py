"""Version management utilities for BenchBox.

This module provides functionality for:
- Version consistency checking across files
- Version reporting and debugging
- Import error handling with version information

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

import json
import re
import sys
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Optional

# Python 3.11+ has tomllib in stdlib, older versions need tomli package
if sys.version_info >= (3, 11):
    import tomllib
else:
    import tomli as tomllib  # type: ignore[import-not-found]

import benchbox

# Cache for parsed pyproject.toml to avoid repeated file reads
_PYPROJECT_CACHE: Optional[dict] = None

# Version sources beyond package metadata that we validate for consistency
_ADDITIONAL_VERSION_PATHS = (
    Path("README.md"),
    Path("docs") / "README.md",
    Path("benchbox") / "utils" / "VERSION_MANAGEMENT.md",
)

PROJECT_ROOT = Path(__file__).parent.parent.parent

# Resolve documentation paths relative to the project root to avoid surprises
DOCUMENTATION_VERSION_PATHS = tuple(PROJECT_ROOT / path for path in _ADDITIONAL_VERSION_PATHS)

_DOC_VERSION_PATTERN = re.compile(
    r"Current\s+release\s*:?\s*`?v?(?P<version>\d+\.\d+\.\d+(?:-[\w\.]+)?)`?",
    re.IGNORECASE,
)


@dataclass(frozen=True)
class VersionConsistencyResult:
    """Detailed outcome of version consistency validation."""

    consistent: bool
    message: str
    expected_version: Optional[str]
    sources: dict[str, Optional[str]]
    normalized_sources: dict[str, Optional[str]]
    missing_sources: tuple[str, ...]
    mismatched_sources: tuple[str, ...]


@dataclass(frozen=True)
class _SemVer:
    """Simple semantic version representation (supports pre-release tags)."""

    major: int
    minor: int
    patch: int
    pre_label: Optional[str] = None
    pre_number: int = 0


_SEMVER_PATTERN = re.compile(
    r"^(?P<major>\d+)\.(?P<minor>\d+)\.(?P<patch>\d+)"
    r"(?:-(?P<label>alpha|beta|rc|dev)(?:\.(?P<number>\d+))?)?$"
)

_PRE_RELEASE_ORDER = {
    None: 5,  # Release versions rank highest
    "rc": 4,
    "beta": 3,
    "alpha": 2,
    "dev": 1,
}


def _parse_semver(version: str) -> _SemVer:
    """Parse a semantic version string.

    Args:
        version: String following the BenchBox semver format.

    Raises:
        ValueError: If the version string is not a valid semantic version.
    """

    match = _SEMVER_PATTERN.match(version)
    if not match:
        raise ValueError(f"Invalid BenchBox semantic version: '{version}'")

    label = match.group("label")
    number_str = match.group("number")

    return _SemVer(
        major=int(match.group("major")),
        minor=int(match.group("minor")),
        patch=int(match.group("patch")),
        pre_label=label,
        pre_number=int(number_str) if number_str is not None else 0,
    )


def _compare_semver(first: _SemVer, second: _SemVer) -> int:
    """Compare two semantic version structures.

    Returns:
        -1 if first < second, 0 if equal, 1 if first > second.
    """

    if first.major != second.major:
        return 1 if first.major > second.major else -1
    if first.minor != second.minor:
        return 1 if first.minor > second.minor else -1
    if first.patch != second.patch:
        return 1 if first.patch > second.patch else -1

    # Handle pre-release ordering (release > pre-release)
    order_first = _PRE_RELEASE_ORDER.get(first.pre_label, 0)
    order_second = _PRE_RELEASE_ORDER.get(second.pre_label, 0)

    if order_first != order_second:
        return 1 if order_first > order_second else -1

    if first.pre_number != second.pre_number:
        return 1 if first.pre_number > second.pre_number else -1

    return 0


def _read_text_safe(path: Path) -> Optional[str]:
    """Read text from a file, returning None if unavailable."""

    try:
        return path.read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError):
        return None


def _document_source_key(path: Path) -> str:
    """Convert an absolute documentation path into a readable source label."""

    try:
        return str(path.relative_to(PROJECT_ROOT))
    except ValueError:
        return str(path)


def _collect_documentation_versions() -> dict[str, Optional[str]]:
    """Collect version markers from documentation files."""

    versions: dict[str, Optional[str]] = {}

    for doc_path in DOCUMENTATION_VERSION_PATHS:
        text = _read_text_safe(doc_path)
        version = None
        if text:
            match = _DOC_VERSION_PATTERN.search(text)
            if match:
                version = match.group("version")
        versions[_document_source_key(doc_path)] = version

    return versions


def _normalize_version(value: Optional[str]) -> Optional[str]:
    """Normalize version markers by stripping prefixes/punctuation."""

    if value is None:
        return None

    candidate = value.strip()
    if not candidate:
        return None

    if candidate.startswith("v") and len(candidate) > 1 and candidate[1].isdigit():
        candidate = candidate[1:]

    candidate = candidate.rstrip("`'\". ")
    return candidate or None


def get_pyproject_version() -> Optional[str]:
    """Get version from pyproject.toml file.

    Returns:
        Version string from pyproject.toml, or None if not found.
    """
    global _PYPROJECT_CACHE

    if _PYPROJECT_CACHE is None:
        try:
            # Find pyproject.toml in project root
            pyproject_path = PROJECT_ROOT / "pyproject.toml"

            if pyproject_path.exists():
                with open(pyproject_path, "rb") as f:
                    _PYPROJECT_CACHE = tomllib.load(f)
            else:
                _PYPROJECT_CACHE = {}
        except Exception:
            # Graceful fallback if file cannot be read
            _PYPROJECT_CACHE = {}

    return _PYPROJECT_CACHE.get("project", {}).get("version")


def get_package_version() -> str:
    """Get version from package __init__.py.

    Returns:
        Version string from benchbox.__version__.
    """
    return benchbox.__version__


def is_version_compatible(
    min_version: Optional[str] = None,
    max_version: Optional[str] = None,
    current_version: Optional[str] = None,
) -> bool:
    """Check if the current BenchBox version falls within the provided bounds.

    Args:
        min_version: Minimum supported version (inclusive).
        max_version: Maximum supported version (inclusive).
        current_version: Override version to check (defaults to package version).

    Returns:
        True if the current version is compatible with the provided range.

    Raises:
        ValueError: If any provided version does not match the expected format.
    """

    version_str = current_version or get_package_version()
    if not version_str:
        return False

    current_semver = _parse_semver(version_str)

    if min_version:
        minimum = _parse_semver(min_version)
        if _compare_semver(current_semver, minimum) < 0:
            return False

    if max_version:
        maximum = _parse_semver(max_version)
        if _compare_semver(current_semver, maximum) > 0:
            return False

    return True


def _gather_version_sources() -> dict[str, Optional[str]]:
    """Collect raw version strings from all tracked sources."""

    versions: dict[str, Optional[str]] = {
        "benchbox.__init__": get_package_version(),
        "pyproject.toml": get_pyproject_version(),
    }
    versions.update(_collect_documentation_versions())
    return versions


def check_version_consistency() -> VersionConsistencyResult:
    """Check if versions are consistent across all sources."""

    versions = _gather_version_sources()
    normalized = {source: _normalize_version(value) for source, value in versions.items()}

    missing_sources = tuple(source for source, value in normalized.items() if value is None)

    # Determine the expected version from the first non-missing entry
    expected_version = next((value for value in normalized.values() if value is not None), None)

    mismatched_sources: tuple[str, ...]
    if expected_version is None:
        mismatched_sources = ()
    else:
        mismatched_sources = tuple(
            source for source, value in normalized.items() if value and value != expected_version
        )

    if expected_version is None:
        message = "No version information found"
        consistent = False
    elif missing_sources:
        missing_list = ", ".join(missing_sources)
        message = f"Missing version markers in: {missing_list}"
        consistent = False
    elif mismatched_sources:
        mismatched_details = {source: versions[source] for source in mismatched_sources}
        message = f"Version mismatch detected: {mismatched_details}"
        consistent = False
    else:
        message = f"All version markers aligned at {expected_version}"
        consistent = True

    return VersionConsistencyResult(
        consistent=consistent,
        message=message,
        expected_version=expected_version,
        sources=versions,
        normalized_sources=normalized,
        missing_sources=missing_sources,
        mismatched_sources=mismatched_sources,
    )


@lru_cache(maxsize=1)
def get_version_info() -> dict[str, object]:
    """Get comprehensive version information for debugging."""

    benchbox_version = get_package_version()
    pyproject_version = get_pyproject_version()
    consistency = check_version_consistency()

    documentation_versions = {
        source: consistency.sources[source]
        for source in consistency.sources
        if source not in {"benchbox.__init__", "pyproject.toml"}
    }

    release_tag = f"v{benchbox_version}" if benchbox_version else "unknown"

    return {
        "benchbox_version": benchbox_version or "unknown",
        "release_tag": release_tag,
        "pyproject_version": pyproject_version or "unknown",
        "documentation_versions": documentation_versions,
        "version_sources": consistency.sources,
        "normalized_version_sources": consistency.normalized_sources,
        "version_consistent": consistency.consistent,
        "version_message": consistency.message,
        "expected_version": consistency.expected_version,
        "missing_sources": consistency.missing_sources,
        "mismatched_sources": consistency.mismatched_sources,
        "python_version": sys.version,
        "python_executable": sys.executable,
        "platform": sys.platform,
    }


def format_version_report(as_json: bool = False, include_system: bool = True) -> str:
    """Format a version report for CLI / diagnostics."""

    info = get_version_info()

    if as_json:
        payload = {
            "benchbox_version": info["benchbox_version"],
            "release_tag": info["release_tag"],
            "pyproject_version": info["pyproject_version"],
            "documentation_versions": info["documentation_versions"],
            "version_consistent": info["version_consistent"],
            "version_message": info["version_message"],
            "expected_version": info["expected_version"],
            "missing_sources": info["missing_sources"],
            "mismatched_sources": info["mismatched_sources"],
        }

        if include_system:
            payload.update(
                {
                    "python_version": info["python_version"],
                    "python_executable": info["python_executable"],
                    "platform": info["platform"],
                }
            )

        return json.dumps(payload, indent=2, sort_keys=True)

    documentation_versions = info.get("documentation_versions", {})
    consistency = "Yes" if info.get("version_consistent") else "No"

    lines = [
        f"BenchBox Version: {info['benchbox_version']}",
        f"Release Tag: {info['release_tag']}",
        f"pyproject.toml Version: {info['pyproject_version']}",
    ]

    for source, version in documentation_versions.items():
        lines.append(f"{source} Version: {version}")

    lines.extend(
        [
            f"Version Consistency: {consistency}",
            f"Status: {info['version_message']}",
        ]
    )

    if include_system:
        lines.extend(
            [
                "",
                f"Python Version: {info['python_version']}",
                f"Python Executable: {info['python_executable']}",
                f"Platform: {info['platform']}",
            ]
        )

    return "\n".join(lines)


def ensure_version_compatible(
    min_version: Optional[str] = None,
    max_version: Optional[str] = None,
    current_version: Optional[str] = None,
) -> None:
    """Validate that the BenchBox version is within expected bounds.

    Args:
        min_version: Minimum supported version (inclusive).
        max_version: Maximum supported version (inclusive).
        current_version: Override version to check (defaults to package version).

    Raises:
        RuntimeError: If the current version is outside of the supported range.
        ValueError: If a provided version string is invalid.
    """

    if is_version_compatible(min_version=min_version, max_version=max_version, current_version=current_version):
        return

    info = get_version_info()
    version_range_parts = []
    if min_version:
        version_range_parts.append(f">= {min_version}")
    if max_version:
        version_range_parts.append(f"<= {max_version}")
    version_range = " and ".join(version_range_parts) if version_range_parts else "the supported range"

    raise RuntimeError(
        "BenchBox version compatibility check failed. "
        f"Current version {info['benchbox_version']} is outside of {version_range}."
    )


def reset_version_cache() -> None:
    """Clear cached version metadata (useful for tests and tooling)."""

    global _PYPROJECT_CACHE
    _PYPROJECT_CACHE = None
    get_version_info.cache_clear()


def validate_version_consistency() -> None:
    """Validate version consistency and raise error if inconsistent.

    Raises:
        RuntimeError: If versions are inconsistent across files.
    """
    consistency = check_version_consistency()

    if consistency.consistent:
        return

    details = {
        "sources": consistency.sources,
        "expected": consistency.expected_version,
        "missing": consistency.missing_sources,
        "mismatched": consistency.mismatched_sources,
    }

    raise RuntimeError(
        "Version inconsistency detected. "
        f"{consistency.message}. "
        f"Details: {details}. "
        "Please ensure benchbox/__init__.py, pyproject.toml, and documentation release markers are aligned."
    )


class ImportErrorWithVersion(ImportError):
    """Enhanced ImportError that includes version information for debugging."""

    def __init__(self, message: str, original_error: Optional[Exception] = None):
        """Initialize enhanced import error.

        Args:
            message: Error message describing the import failure.
            original_error: Original exception that caused the import failure.
        """
        version_info = get_version_info()
        enhanced_message = (
            f"{message}\n\n"
            f"Version Information:\n"
            f"  BenchBox: {version_info['benchbox_version']}\n"
            f"  Release: {version_info['release_tag']}\n"
            f"  Python: {version_info['python_version']}\n"
            f"  Platform: {version_info['platform']}\n"
        )

        if original_error:
            enhanced_message += f"\nOriginal error: {original_error}"

        super().__init__(enhanced_message)
        self.original_error = original_error
        self.version_info = version_info


def create_import_error(
    benchmark_name: str, missing_dependencies: Optional[list] = None, original_error: Optional[Exception] = None
) -> ImportErrorWithVersion:
    """Create an enhanced import error with helpful information.

    Args:
        benchmark_name: Name of the benchmark that failed to import.
        missing_dependencies: List of missing dependencies that might fix the issue.
        original_error: Original exception that caused the import failure.

    Returns:
        Enhanced ImportError with version and dependency information.
    """
    message = f"Could not import benchmark '{benchmark_name}'"

    if missing_dependencies:
        deps_str = ", ".join(sorted(missing_dependencies))
        message += f". Optional dependency extras required: {deps_str}"
        suggestions = [f"uv add benchbox --extra {extra}" for extra in missing_dependencies]
        combined_extras = " ".join(f"--extra {extra}" for extra in sorted(missing_dependencies))
        combined = f"uv add benchbox {combined_extras}"
        if len(missing_dependencies) > 1:
            suggestions.append(combined)
        message += "\n\nSuggested installs:\n  " + "\n  ".join(sorted(set(suggestions)))

    return ImportErrorWithVersion(message, original_error)
