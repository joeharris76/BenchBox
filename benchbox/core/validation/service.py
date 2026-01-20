"""Core validation orchestration service.

Provides programmatic access to the full validation workflow (preflight,
manifest, database, and platform capability checks) without relying on CLI
modules. This enables benchmarks, adapters, and external automation to trigger
validation directly from the core package.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .engines import (
    DatabaseValidationEngine,
    DataValidationEngine,
    ValidationResult,
    ValidationSummary,
)


@dataclass(frozen=True)
class PlatformValidationResult:
    """Container for platform capability and optional connection health checks."""

    capabilities: ValidationResult
    connection_health: ValidationResult | None = None


class ValidationService:
    """Core validation service coordinating data and database checks."""

    def __init__(
        self,
        *,
        data_engine: DataValidationEngine | None = None,
        db_engine: DatabaseValidationEngine | None = None,
    ) -> None:
        self._data_engine = data_engine or DataValidationEngine()
        self._db_engine = db_engine or DatabaseValidationEngine()

    # ------------------------------------------------------------------
    # Individual validation helpers
    # ------------------------------------------------------------------
    def run_preflight(
        self,
        benchmark_type: str,
        scale_factor: float,
        output_dir: Path,
    ) -> ValidationResult:
        """Validate that the environment is ready for data generation."""

        return self._data_engine.validate_preflight_conditions(benchmark_type, scale_factor, output_dir)

    def run_manifest(self, manifest_path: Path) -> ValidationResult:
        """Validate generated data files referenced by the manifest."""

        return self._data_engine.validate_generated_data(manifest_path)

    def run_database(
        self,
        connection: Any,
        benchmark_type: str,
        scale_factor: float,
    ) -> ValidationResult:
        """Validate loaded database contents for a benchmark."""

        return self._db_engine.validate_loaded_data(connection, benchmark_type, scale_factor)

    def run_platform(
        self,
        platform_adapter: Any,
        benchmark_type: str,
        *,
        connection: Any | None = None,
    ) -> PlatformValidationResult:
        """Validate platform capabilities and optional connection health."""

        capabilities = platform_adapter.validate_platform_capabilities(benchmark_type)
        connection_health = None
        health_checker = getattr(platform_adapter, "validate_connection_health", None)
        if connection is not None and callable(health_checker):
            connection_health = health_checker(connection)

        return PlatformValidationResult(capabilities=capabilities, connection_health=connection_health)

    # ------------------------------------------------------------------
    # Composite workflows
    # ------------------------------------------------------------------
    def run_comprehensive(
        self,
        *,
        benchmark_type: str,
        scale_factor: float,
        output_dir: Path,
        manifest_path: Path | None = None,
        connection: Any | None = None,
        platform_adapter: Any | None = None,
    ) -> list[ValidationResult]:
        """Execute the full validation workflow and return collected results."""

        results: list[ValidationResult] = []

        preflight = self.run_preflight(benchmark_type, scale_factor, output_dir)
        results.append(preflight)

        if platform_adapter is not None:
            platform_result = self.run_platform(platform_adapter, benchmark_type, connection=connection)
            results.append(platform_result.capabilities)
            if platform_result.connection_health is not None:
                results.append(platform_result.connection_health)

        if manifest_path is not None and manifest_path.exists():
            manifest_result = self.run_manifest(manifest_path)
            results.append(manifest_result)

        if connection is not None:
            db_result = self.run_database(connection, benchmark_type, scale_factor)
            results.append(db_result)

        return results

    # ------------------------------------------------------------------
    # Summary helpers
    # ------------------------------------------------------------------
    @staticmethod
    def summarize(results: Sequence[ValidationResult]) -> ValidationSummary:
        """Build a validation summary from a sequence of validation results."""

        filtered: list[ValidationResult] = [res for res in results if res is not None]
        total = len(filtered)
        passed = sum(1 for res in filtered if res.is_valid)
        failed = total - passed
        warnings = sum(len(res.warnings) for res in filtered)

        return ValidationSummary(
            total_validations=total,
            passed_validations=passed,
            failed_validations=failed,
            warnings_count=warnings,
        )
