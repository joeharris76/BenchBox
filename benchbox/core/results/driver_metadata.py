"""Shared driver metadata propagation helpers for benchmark results."""

from __future__ import annotations

from typing import Any


def apply_driver_metadata(
    result: Any,
    *,
    database_config: Any = None,
    platform_adapter: Any = None,
) -> None:
    """Propagate driver/runtime metadata onto a benchmark result object.

    This helper is intentionally duck-typed so it can be reused by both
    orchestrator/lifecycle and legacy execution compatibility code.
    """

    if result is None:
        return

    driver_package = None
    driver_version_requested = None
    driver_version_resolved = None
    driver_version_actual = None
    driver_runtime_strategy = None
    driver_runtime_path = None
    driver_runtime_python_executable = None
    auto_install_used = False

    if platform_adapter is not None:
        driver_package = getattr(platform_adapter, "driver_package", None) or driver_package
        driver_version_requested = (
            getattr(platform_adapter, "driver_version_requested", None) or driver_version_requested
        )
        driver_version_resolved = getattr(platform_adapter, "driver_version_resolved", None) or driver_version_resolved
        driver_version_actual = getattr(platform_adapter, "driver_version_actual", None) or driver_version_actual
        driver_runtime_strategy = getattr(platform_adapter, "driver_runtime_strategy", None) or driver_runtime_strategy
        driver_runtime_path = getattr(platform_adapter, "driver_runtime_path", None) or driver_runtime_path
        driver_runtime_python_executable = (
            getattr(platform_adapter, "driver_runtime_python_executable", None) or driver_runtime_python_executable
        )
        auto_install_used = getattr(platform_adapter, "driver_auto_install_used", False) or auto_install_used

    if database_config is not None:
        driver_package = driver_package or getattr(database_config, "driver_package", None)
        config_driver_version = getattr(database_config, "driver_version", None)
        driver_version_requested = driver_version_requested or config_driver_version
        db_resolved = getattr(database_config, "driver_version_resolved", None)
        driver_version_resolved = driver_version_resolved or db_resolved or config_driver_version
        driver_version_actual = driver_version_actual or getattr(database_config, "driver_version_actual", None)
        driver_runtime_strategy = driver_runtime_strategy or getattr(database_config, "driver_runtime_strategy", None)
        driver_runtime_path = driver_runtime_path or getattr(database_config, "driver_runtime_path", None)
        driver_runtime_python_executable = driver_runtime_python_executable or getattr(
            database_config, "driver_runtime_python_executable", None
        )
        auto_install_used = auto_install_used or bool(getattr(database_config, "driver_auto_install", False))
        if (
            driver_version_resolved
            and getattr(database_config, "driver_version_resolved", None) != driver_version_resolved
        ):
            database_config.driver_version_resolved = driver_version_resolved

    if hasattr(result, "driver_package"):
        result.driver_package = driver_package
    if hasattr(result, "driver_version_requested"):
        result.driver_version_requested = driver_version_requested
    if hasattr(result, "driver_version_resolved"):
        result.driver_version_resolved = driver_version_resolved
    if hasattr(result, "driver_version_actual"):
        result.driver_version_actual = driver_version_actual
    if hasattr(result, "driver_runtime_strategy"):
        result.driver_runtime_strategy = driver_runtime_strategy
    if hasattr(result, "driver_runtime_path"):
        result.driver_runtime_path = driver_runtime_path
    if hasattr(result, "driver_runtime_python_executable"):
        result.driver_runtime_python_executable = driver_runtime_python_executable
    if hasattr(result, "driver_auto_install"):
        result.driver_auto_install = auto_install_used

    execution_metadata = getattr(result, "execution_metadata", None)
    if isinstance(execution_metadata, dict):
        if driver_package:
            execution_metadata.setdefault("driver_package", driver_package)
        if driver_version_requested:
            execution_metadata.setdefault("driver_version_requested", driver_version_requested)
        if driver_version_resolved:
            execution_metadata["driver_version_resolved"] = driver_version_resolved
        if driver_version_actual:
            execution_metadata["driver_version_actual"] = driver_version_actual
        if driver_runtime_strategy:
            execution_metadata["driver_runtime_strategy"] = driver_runtime_strategy
        if driver_runtime_path:
            execution_metadata["driver_runtime_path"] = driver_runtime_path
        if driver_runtime_python_executable:
            execution_metadata["driver_runtime_python_executable"] = driver_runtime_python_executable
        execution_metadata.setdefault("driver_auto_install_used", auto_install_used)
