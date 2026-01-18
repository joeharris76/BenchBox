"""Runtime environment helpers for platform driver management.

This module centralizes driver version enforcement so platform adapters
can honour requested package versions before importing heavy dependencies.
"""

from __future__ import annotations

import logging
import os
import shlex
import subprocess
import sys
from dataclasses import dataclass

try:
    from importlib import metadata as importlib_metadata
except ImportError:  # pragma: no cover
    import importlib_metadata  # type: ignore[assignment]

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class DriverResolution:
    """Represents the outcome of a driver version resolution."""

    package: str
    requested: str | None
    resolved: str | None
    auto_install_used: bool


def _normalize_package_name(package: str) -> str:
    return package.replace("_", "-")


def _get_installed_version(package: str) -> str | None:
    try:
        return importlib_metadata.version(package)
    except importlib_metadata.PackageNotFoundError:
        return None


def _run_install_command(command: list[str]) -> None:
    logger.debug("Running install command: %s", " ".join(shlex.quote(part) for part in command))
    result = subprocess.run(
        command,
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )

    if result.returncode != 0:
        output = result.stdout.strip()
        raise RuntimeError(
            f"Driver auto-install failed. Command: {' '.join(shlex.quote(part) for part in command)}\n{output}"
        )


def _should_auto_install(auto_install: bool) -> bool:
    if auto_install:
        return True
    env_flag = os.getenv("BENCHBOX_DRIVER_AUTO_INSTALL", "").strip().lower()
    return env_flag in {"1", "true", "yes", "on"}


def ensure_driver_version(
    *,
    package_name: str | None,
    requested_version: str | None,
    auto_install: bool = False,
    install_hint: str | None = None,
) -> DriverResolution:
    """Ensure the specified driver package matches the requested version.

    Args:
        package_name: Distribution name for the driver package.
        requested_version: Exact version string requested by the user/config.
        auto_install: Whether BenchBox may attempt installation automatically.
        install_hint: Human-friendly installation command for error messages.

    Returns:
        DriverResolution describing the resolved version.

    Raises:
        RuntimeError: If the driver version is not satisfied and cannot be installed.
    """

    if not package_name:
        return DriverResolution(package="", requested=requested_version, resolved=None, auto_install_used=False)

    normalized_package = _normalize_package_name(package_name)
    requested = (requested_version or "").strip() or None

    installed_version = _get_installed_version(normalized_package)
    logger.debug(
        "Detected driver package %s version %s (requested %s)",
        normalized_package,
        installed_version,
        requested,
    )

    # Nothing requested: ensure something is installed and report it.
    if requested is None:
        if installed_version is None:
            hint = install_hint or f"uv add {normalized_package}"
            raise RuntimeError(
                f"Driver package '{normalized_package}' is not installed. "
                "Install it or specify a driver_version in the configuration." + f"\nSuggested command: {hint}"
            )
        return DriverResolution(
            package=normalized_package,
            requested=None,
            resolved=installed_version,
            auto_install_used=False,
        )

    # Requested version already satisfied.
    if installed_version == requested:
        return DriverResolution(
            package=normalized_package,
            requested=requested,
            resolved=installed_version,
            auto_install_used=False,
        )

    # Requested version missing or different.
    auto_install_enabled = _should_auto_install(auto_install)
    if not auto_install_enabled:
        hint = install_hint or f"uv add {normalized_package}=={requested}"
        raise RuntimeError(
            "Driver package '{package}' is at version {installed} but version {requested} was requested. "
            "Install the correct version or enable driver_auto_install.".format(
                package=normalized_package,
                installed=installed_version or "not installed",
                requested=requested,
            )
            + f"\nSuggested command: {hint}"
        )

    # Attempt auto-install using uv; fall back to pip if uv missing.
    commands_to_try = [
        ["uv", "pip", "install", f"{normalized_package}=={requested}"],
        [sys.executable, "-m", "pip", "install", f"{normalized_package}=={requested}"],
    ]

    last_error: Exception | None = None
    for command in commands_to_try:
        try:
            _run_install_command(command)
            installed_version = _get_installed_version(normalized_package)
            if installed_version == requested:
                return DriverResolution(
                    package=normalized_package,
                    requested=requested,
                    resolved=installed_version,
                    auto_install_used=True,
                )
        except Exception as exc:  # pragma: no cover - exercised in tests via mocks
            last_error = exc
            logger.warning("Driver auto-install command failed: %s", exc)

    hint = install_hint or f"uv add {normalized_package}=={requested}"
    base_message = (
        f"Failed to install driver package '{normalized_package}' at version {requested}. "
        "Install the package manually and retry."
    )
    if last_error:
        base_message += f"\nReason: {last_error}"
    base_message += f"\nSuggested command: {hint}"
    raise RuntimeError(base_message)
