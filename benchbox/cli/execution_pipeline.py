"""Refactored CLI execution architecture using pipeline pattern.

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Callable, Optional

from rich.console import Console

from benchbox.core.config import BenchmarkConfig, DatabaseConfig, RunConfig, SystemProfile
from benchbox.core.constants import (
    GENERIC_POWER_DEFAULT_MEASUREMENT_ITERATIONS,
    GENERIC_POWER_DEFAULT_WARMUP_ITERATIONS,
)
from benchbox.core.results.models import BenchmarkResults
from benchbox.utils.printing import quiet_console

# Note: Core lifecycle orchestration lives in benchbox.core.runner.


logger = logging.getLogger(__name__)


@dataclass
class ExecutionContext:
    """Context passed through execution pipeline."""

    benchmark_config: BenchmarkConfig
    database_config: Optional[DatabaseConfig] = None
    system_profile: Optional[SystemProfile] = None
    run_config: Optional[RunConfig] = None
    benchmark_instance: Optional[Any] = None
    platform_adapter: Optional[Any] = None
    result: Optional[BenchmarkResults] = None

    # Execution state
    stage: str = "initializing"
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)


class ExecutionStage(ABC):
    """Abstract base class for execution pipeline stages."""

    def __init__(self, name: str):
        self.name = name
        self.logger = logging.getLogger(f"{__name__}.{name}")

    @abstractmethod
    def execute(self, context: ExecutionContext) -> ExecutionContext:
        """Execute this stage of the pipeline."""

    def can_execute(self, context: ExecutionContext) -> bool:
        """Check if this stage can execute given the current context."""
        return True

    def on_error(self, context: ExecutionContext, error: Exception) -> ExecutionContext:
        """Handle errors that occur during stage execution."""
        context.errors.append(f"{self.name}: {str(error)}")
        self.logger.error(f"Stage {self.name} failed: {error}")
        return context


class ConfigurationValidationStage(ExecutionStage):
    """Validates benchmark and database configuration."""

    def __init__(self):
        super().__init__("configuration_validation")

    def execute(self, context: ExecutionContext) -> ExecutionContext:
        """Validate configuration."""
        context.stage = "validating_configuration"

        # Validate benchmark config
        if not context.benchmark_config.name:
            context.errors.append("Benchmark name is required")

        if context.benchmark_config.scale_factor <= 0:
            context.errors.append("Scale factor must be positive")

        # Create default run config if not provided
        if context.run_config is None:
            opts = context.benchmark_config.options or {}
            iterations = int(
                opts.get("power_iterations", GENERIC_POWER_DEFAULT_MEASUREMENT_ITERATIONS)
                or GENERIC_POWER_DEFAULT_MEASUREMENT_ITERATIONS
            )
            warmups = int(
                opts.get("power_warmup_iterations", GENERIC_POWER_DEFAULT_WARMUP_ITERATIONS)
                or GENERIC_POWER_DEFAULT_WARMUP_ITERATIONS
            )
            fail_fast = bool(opts.get("power_fail_fast", False))

            context.run_config = RunConfig(
                query_subset=context.benchmark_config.queries,
                concurrent_streams=context.benchmark_config.concurrency,
                test_execution_type=context.benchmark_config.test_execution_type,
                scale_factor=context.benchmark_config.scale_factor,
                capture_plans=context.benchmark_config.capture_plans,
                strict_plan_capture=context.benchmark_config.strict_plan_capture,
                iterations=max(1, iterations),
                warm_up_iterations=max(0, warmups),
                power_fail_fast=fail_fast,
            )

        return context


class BenchmarkLoadingStage(ExecutionStage):
    """Loads benchmark instance."""

    def __init__(self):
        super().__init__("benchmark_loading")

    def execute(self, context: ExecutionContext) -> ExecutionContext:
        """Load benchmark instance."""
        context.stage = "loading_benchmark"

        # Dynamic benchmark loading
        benchmark_instance = self._load_benchmark_instance(context.benchmark_config)
        if benchmark_instance is None:
            context.errors.append(f"Failed to load benchmark: {context.benchmark_config.name}")
            return context

        context.benchmark_instance = benchmark_instance
        return context

    def _load_benchmark_instance(self, config: BenchmarkConfig):
        """Load benchmark instance via core loader (no direct imports)."""
        try:
            from benchbox.core.benchmark_loader import get_benchmark_instance

            return get_benchmark_instance(config, system_profile=None)  # type: ignore[arg-type]
        except Exception as e:
            logger.error(f"Failed to load benchmark {config.name}: {e}")
            return None


class PlatformAdapterStage(ExecutionStage):
    """Sets up platform adapter for database execution."""

    def __init__(self):
        super().__init__("platform_adapter")

    def can_execute(self, context: ExecutionContext) -> bool:
        """Only execute if database config is provided."""
        return (
            context.database_config is not None
            and context.run_config is not None
            and context.run_config.test_execution_type != "data_only"
        )

    def execute(self, context: ExecutionContext) -> ExecutionContext:
        """Set up platform adapter."""
        context.stage = "setting_up_platform"

        # Assert database_config exists (checked by can_execute)
        assert context.database_config is not None, "database_config must not be None"

        try:
            from benchbox.platforms import get_platform_adapter

            # Get platform adapter with config as keyword arguments
            # DatabaseConfig is a Pydantic BaseModel, use model_dump()
            config_dict = context.database_config.model_dump()

            platform_adapter = get_platform_adapter(context.database_config.type, **config_dict)

            if platform_adapter is None:
                context.errors.append(f"No platform adapter for {context.database_config.type}")
                return context

            # Set benchmark instance on adapter for database validation
            # This allows the adapter to validate schema compatibility when checking
            # if an existing database can be reused
            if context.benchmark_instance:
                platform_adapter.benchmark_instance = context.benchmark_instance
                platform_adapter.scale_factor = context.benchmark_config.scale_factor

            context.platform_adapter = platform_adapter

        except Exception as e:
            context.errors.append(f"Platform adapter setup failed: {str(e)}")

        return context


class BenchmarkExecutionStage(ExecutionStage):
    """Executes the actual benchmark."""

    def __init__(self):
        super().__init__("benchmark_execution")

    def execute(self, context: ExecutionContext) -> ExecutionContext:
        """Execute benchmark."""
        context.stage = "executing_benchmark"

        # Determine test type (default to "standard" if run_config is None)
        test_type = context.run_config.test_execution_type if context.run_config else "standard"

        if test_type == "data_only":
            context.result = self._execute_data_only(context)
        else:
            # For all database-backed modes (power/throughput/standard/etc.),
            # delegate to the platform adapter directly using unified kwargs.
            context.result = self._execute_with_platform(context)

        return context

    def _execute_data_only(self, context: ExecutionContext) -> BenchmarkResults:
        """Execute in data-only mode, returning enhanced benchmark results."""

        if context.benchmark_instance is None:
            raise RuntimeError("Benchmark instance is required for data-only execution")

        data_artifacts = context.benchmark_instance.generate_data()
        artifact_list = self._normalize_artifacts(data_artifacts)

        phases = {
            "data_generation": {
                "status": "COMPLETED",
                "artifacts_generated": len(artifact_list),
            }
        }

        execution_metadata = {
            "mode": "data_only",
            "generated_artifacts": artifact_list,
            "benchmark_id": context.benchmark_config.name,
        }

        result = context.benchmark_instance.create_enhanced_benchmark_result(
            platform="data_only",
            query_results=[],
            duration_seconds=0.0,
            phases=phases,
            execution_metadata=execution_metadata,
            validation_status="PASSED",
            validation_details={"mode": "data_generation"},
            test_execution_type="data_only",
        )

        result.test_execution_type = "data_only"
        result.validation_status = "PASSED"
        result.validation_details = {"mode": "data_generation"}
        result.execution_metadata = execution_metadata
        result._benchmark_id_override = context.benchmark_config.name
        result.resource_utilization = getattr(result, "resource_utilization", {})
        if not isinstance(result, BenchmarkResults):
            result.benchmark_id = context.benchmark_config.name
        return result

    @staticmethod
    def _normalize_artifacts(data_artifacts: Any) -> list[str]:
        """Normalize data generation outputs into a list of artifact identifiers."""

        if data_artifacts is None:
            return []

        if isinstance(data_artifacts, dict):
            normalized: list[str] = []
            for value in data_artifacts.values():
                if isinstance(value, (list, tuple, set)):
                    normalized.extend(str(item) for item in value)
                else:
                    normalized.append(str(value))
            return normalized

        if isinstance(data_artifacts, (list, tuple, set)):
            return [str(item) for item in data_artifacts]

        return [str(data_artifacts)]

    def _execute_with_platform(self, context: ExecutionContext) -> BenchmarkResults:
        """Execute with platform adapter."""
        if context.platform_adapter is None:
            raise RuntimeError("Platform adapter required for database execution")

        # Use platform adapter for execution
        kwargs = context.run_config.__dict__ if context.run_config else {}
        # Avoid conflicting keyword for positional benchmark parameter
        if "benchmark" in kwargs:
            kwargs = {k: v for k, v in kwargs.items() if k != "benchmark"}
        return context.platform_adapter.run_benchmark(context.benchmark_instance, **kwargs)


class ExecutionPipeline:
    """Manages execution pipeline stages."""

    def __init__(self, console: Optional[Console] = None):
        self.console = console or quiet_console
        self.stages: list[ExecutionStage] = [
            ConfigurationValidationStage(),
            BenchmarkLoadingStage(),
            PlatformAdapterStage(),
            BenchmarkExecutionStage(),
        ]
        self.progress_callback: Optional[Callable[[str, ExecutionContext], None]] = None

    def set_progress_callback(self, callback: Callable[[str, ExecutionContext], None]):
        """Set callback for progress updates."""
        self.progress_callback = callback

    def execute(self, context: ExecutionContext) -> ExecutionContext:
        """Execute full pipeline."""
        logger.info(f"Starting execution pipeline for {context.benchmark_config.name}")

        for stage in self.stages:
            if not stage.can_execute(context):
                logger.debug(f"Skipping stage {stage.name} - conditions not met")
                continue

            try:
                logger.debug(f"Executing stage: {stage.name}")

                # Progress callback
                if self.progress_callback:
                    self.progress_callback(stage.name, context)

                context = stage.execute(context)

                # Stop on errors
                if context.errors:
                    logger.error(f"Pipeline stopped due to errors: {context.errors}")
                    break

            except Exception as e:
                context = stage.on_error(context, e)
                logger.error(f"Stage {stage.name} failed with exception: {e}")
                break

        logger.info(f"Pipeline completed with stage: {context.stage}")
        return context


class ExecutionEngine:
    """Main execution engine using pipeline architecture."""

    def __init__(self, console: Optional[Console] = None):
        self.console = console or quiet_console
        self.pipeline = ExecutionPipeline(console)
        self.pipeline.set_progress_callback(self._on_progress)
        self._last_context: Optional[ExecutionContext] = None

    def execute_benchmark(
        self,
        benchmark_config: BenchmarkConfig,
        database_config: Optional[DatabaseConfig] = None,
        system_profile: Optional[SystemProfile] = None,
        run_config: Optional[RunConfig] = None,
    ) -> BenchmarkResults:
        """Execute benchmark using pipeline architecture."""

        # Create execution context
        context = ExecutionContext(
            benchmark_config=benchmark_config,
            database_config=database_config,
            system_profile=system_profile,
            run_config=run_config,
        )
        self._last_context = context

        # Execute pipeline
        try:
            context = self.pipeline.execute(context)
        finally:
            self._last_context = context

        # Handle results
        if context.errors:
            error_msg = "; ".join(context.errors)
            raise RuntimeError(f"Execution failed: {error_msg}")

        if context.result is None:
            raise RuntimeError("Execution completed but no result generated")

        self._enrich_driver_metadata(context)
        self._last_context = context
        return context.result

    def _on_progress(self, stage_name: str, context: ExecutionContext):
        """Handle progress updates."""
        stage_display = {
            "configuration_validation": "Validating configuration",
            "benchmark_loading": "Loading benchmark",
            "platform_adapter": "Setting up database connection",
            "benchmark_execution": "Executing benchmark",
        }

        display_name = stage_display.get(stage_name, stage_name)
        self.console.print(f"[blue]▶[/blue] {display_name}...")

    def _enrich_driver_metadata(self, context: ExecutionContext) -> None:
        """Propagate driver version metadata onto the final result."""

        result = context.result
        if result is None:
            return

        db_config = context.database_config
        adapter = context.platform_adapter

        driver_package = None
        driver_version_requested = None
        driver_version_resolved = None
        auto_install_used = False

        if adapter is not None:
            driver_package = getattr(adapter, "driver_package", None) or driver_package
            driver_version_requested = getattr(adapter, "driver_version_requested", None) or driver_version_requested
            driver_version_resolved = getattr(adapter, "driver_version_resolved", None) or driver_version_resolved
            auto_install_used = getattr(adapter, "driver_auto_install_used", False) or auto_install_used

        if db_config is not None:
            driver_package = driver_package or db_config.driver_package
            driver_version_requested = driver_version_requested or db_config.driver_version
            driver_version_resolved = (
                driver_version_resolved or db_config.driver_version_resolved or db_config.driver_version
            )
            auto_install_used = auto_install_used or db_config.driver_auto_install
            if driver_version_resolved and db_config.driver_version_resolved != driver_version_resolved:
                db_config.driver_version_resolved = driver_version_resolved

        if hasattr(result, "driver_package"):
            result.driver_package = driver_package
        if hasattr(result, "driver_version_requested"):
            result.driver_version_requested = driver_version_requested
        if hasattr(result, "driver_version_resolved"):
            result.driver_version_resolved = driver_version_resolved
        if hasattr(result, "driver_auto_install"):
            result.driver_auto_install = auto_install_used

        # Surface in execution metadata when available
        execution_metadata = getattr(result, "execution_metadata", None)
        if isinstance(execution_metadata, dict):
            if driver_package:
                execution_metadata.setdefault("driver_package", driver_package)
            if driver_version_requested:
                execution_metadata.setdefault("driver_version_requested", driver_version_requested)
            if driver_version_resolved:
                execution_metadata["driver_version_resolved"] = driver_version_resolved
            execution_metadata.setdefault("driver_auto_install_used", auto_install_used)

    @property
    def last_context(self) -> Optional[ExecutionContext]:
        """Return the most recent execution context (may be partial on failure)."""

        return self._last_context

    def get_benchmark_instance(self) -> Optional[Any]:
        """Return the benchmark instance from the most recent context."""

        if self._last_context is None:
            return None
        return self._last_context.benchmark_instance


def create_execution_engine(console: Optional[Console] = None) -> ExecutionEngine:
    """Factory function to create execution engine."""
    return ExecutionEngine(console)
