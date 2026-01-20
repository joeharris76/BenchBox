"""
Core benchmark lifecycle runner (generate → optional validate → load → execute).

This module provides a reusable, CLI‑independent orchestration API that executes
benchmark lifecycles using core types and platform adapters. It is designed to be
used by both programmatic clients and the CLI wrapper.
"""

from __future__ import annotations

import contextlib
import json
import logging
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from benchbox.core.benchmark_loader import (
    get_benchmark_instance,
)
from benchbox.core.config import (
    BenchmarkConfig,
    DatabaseConfig,
    RunConfig,
    SystemProfile,
)
from benchbox.core.constants import (
    GENERIC_POWER_DEFAULT_MEASUREMENT_ITERATIONS,
    GENERIC_POWER_DEFAULT_WARMUP_ITERATIONS,
)
from benchbox.core.results.models import (
    BenchmarkResults,
)
from benchbox.core.runner.conversion import FormatConversionOrchestrator
from benchbox.core.validation import ValidationResult
from benchbox.monitoring import PerformanceMonitor, ResourceMonitor, attach_snapshot_to_result
from benchbox.platforms import get_platform_adapter
from benchbox.utils.cloud_storage import create_path_handler
from benchbox.utils.format_converters import ConversionOptions
from benchbox.utils.verbosity import VerbosityMixin, VerbositySettings

logger = logging.getLogger(__name__)


_STATUS_PRIORITY = {
    "FAILED": 4,
    "WARNINGS": 3,
    "PASSED": 2,
    "NOT_RUN": 1,
    "UNKNOWN": 0,
}


def _resolve_manifest_allowed_names(benchmark: Any, config: BenchmarkConfig) -> set[str]:
    """Return acceptable benchmark identifiers for manifest validation."""

    allowed = {config.name.lower()}

    getter = getattr(benchmark, "get_data_source_benchmark", None)
    if callable(getter):
        try:
            alias = getter()
        except NotImplementedError:
            alias = None
        except Exception as exc:  # pragma: no cover - defensive logging
            logger.warning(
                "data sharing alias resolution failed for %s: %s",
                getattr(benchmark, "__class__", type(benchmark)).__name__,
                exc,
            )
            alias = None

        if alias:
            if isinstance(alias, str):
                allowed.add(alias.lower())
            else:
                logger.warning(
                    "Unexpected alias type %s from %s; ignoring.",
                    type(alias),
                    getattr(benchmark, "__class__", type(benchmark)).__name__,
                )

    return allowed


def _resolve_output_dir_handler(benchmark: Any, output_root: str | None) -> Any | None:
    """Resolve and cache the benchmark output directory handler if available."""

    if output_root:
        handler = create_path_handler(output_root)
        benchmark.output_dir = handler
        return handler

    existing = getattr(benchmark, "output_dir", None)
    if existing is None:
        return None

    handler = create_path_handler(existing)
    benchmark.output_dir = handler
    return handler


def _run_preflight_validation(
    benchmark: Any, benchmark_config: BenchmarkConfig, output_dir_handler: Any
) -> ValidationResult:
    """Execute preflight validation using benchmark helpers or core engine."""

    benchmark_name = getattr(benchmark_config, "name", getattr(benchmark, "name", "")).lower()
    scale_factor = getattr(benchmark_config, "scale_factor", getattr(benchmark, "scale_factor", 1.0))

    if hasattr(benchmark, "validate_preflight"):
        return benchmark.validate_preflight(output_dir=output_dir_handler, benchmark_name=benchmark_name)

    from benchbox.core.validation import DataValidationEngine

    engine = DataValidationEngine()
    if output_dir_handler is None:
        raise RuntimeError("Output directory is required for preflight validation but was not provided.")
    return engine.validate_preflight_conditions(benchmark_name, scale_factor, output_dir_handler)


def _run_manifest_validation(benchmark: Any, benchmark_config: BenchmarkConfig) -> ValidationResult:
    """Validate the generated data manifest if present."""

    if hasattr(benchmark, "validate_manifest"):
        return benchmark.validate_manifest(benchmark_name=benchmark_config.name)

    from benchbox.core.validation import DataValidationEngine, ValidationResult

    output_dir = getattr(benchmark, "output_dir", None)
    if output_dir is None or not hasattr(output_dir, "joinpath"):
        return ValidationResult(
            is_valid=False,
            errors=["Benchmark output directory not configured; cannot validate manifest"],
            warnings=[],
            details={"benchmark": benchmark_config.name},
        )

    manifest_path = output_dir.joinpath("_datagen_manifest.json")
    engine = DataValidationEngine()
    return engine.validate_generated_data(manifest_path)


def _run_postload_validation(
    adapter: Any,
    benchmark_config: BenchmarkConfig,
    platform_config: dict[str, Any] | None,
) -> ValidationResult | None:
    """Execute post-load validation using the core database validation engine."""

    if adapter is None:
        return None

    from benchbox.core.validation import DatabaseValidationEngine, ValidationResult

    if not hasattr(adapter, "create_connection") or not hasattr(adapter, "close_connection"):
        return ValidationResult(
            is_valid=False,
            errors=["Platform adapter does not support connection-based validation"],
            warnings=[],
            details={"benchmark": benchmark_config.name},
        )

    connection = None
    try:
        connection = adapter.create_connection(**(platform_config or {}))
        engine = DatabaseValidationEngine()
        return engine.validate_loaded_data(connection, benchmark_config.name.lower(), benchmark_config.scale_factor)
    except Exception as exc:  # pragma: no cover - defensive safeguard
        return ValidationResult(
            is_valid=False,
            errors=[f"Post-load validation failed: {exc}"],
            warnings=[],
            details={"benchmark": benchmark_config.name},
        )
    finally:
        if connection is not None:
            try:
                adapter.close_connection(connection)
            except Exception:  # pragma: no cover - defensive safeguard
                pass


def _finalize_validation_metadata(
    result: BenchmarkResults, records: list[tuple[str, ValidationResult]]
) -> BenchmarkResults:
    """Merge collected validation records into the final result object."""

    if not records:
        current_status = (result.validation_status or "UNKNOWN").upper()
        if current_status in {"", "UNKNOWN", None}:
            result.validation_status = "NOT_RUN"
        result.validation_details = result.validation_details or {}
        result.validation_details.setdefault("stages", [])
        result.validation_details.setdefault("error_count", 0)
        result.validation_details.setdefault("warning_count", 0)
        return result

    stages = list((result.validation_details or {}).get("stages", []))
    error_count = int((result.validation_details or {}).get("error_count", 0))
    warning_count = int((result.validation_details or {}).get("warning_count", 0))

    aggregate_status = "PASSED"
    for stage_name, record in records:
        stage_status = "FAILED" if not record.is_valid else ("WARNINGS" if record.warnings else "PASSED")
        if stage_status == "FAILED":
            aggregate_status = "FAILED"
        elif stage_status == "WARNINGS" and aggregate_status != "FAILED":
            aggregate_status = "WARNINGS"

        stages.append(
            {
                "stage": stage_name,
                "status": stage_status,
                "errors": list(record.errors),
                "warnings": list(record.warnings),
                "details": record.details,
            }
        )
        error_count += len(record.errors)
        warning_count += len(record.warnings)

    result.validation_details = result.validation_details or {}
    result.validation_details.update(
        {
            "stages": stages,
            "error_count": error_count,
            "warning_count": warning_count,
        }
    )

    current_status = (result.validation_status or "UNKNOWN").upper()
    if _STATUS_PRIORITY.get(aggregate_status, 0) >= _STATUS_PRIORITY.get(current_status, 0):
        result.validation_status = aggregate_status
    else:
        result.validation_status = current_status

    return result


def _execute_load_only_mode(
    *,
    benchmark: Any,
    benchmark_config: BenchmarkConfig,
    adapter: Any,
    platform_config: dict[str, Any] | None,
    validation_opts: ValidationOptions,
) -> tuple[BenchmarkResults, ValidationResult | None]:
    """Execute load-only workflow using the core runner primitives."""

    connection = None
    table_stats: dict[str, int] = {}
    load_time = 0.0
    schema_time = 0.0
    postload_result: ValidationResult | None = None

    try:
        connection = adapter.create_connection(**(platform_config or {}))

        # Create schema before loading data
        schema_time = adapter.create_schema(benchmark, connection)

        data_dir = getattr(benchmark, "output_dir", None)
        if data_dir is None:
            data_dir = _resolve_output_dir_handler(benchmark, None)
        if data_dir is None:
            raise RuntimeError("Benchmark output directory not configured; cannot perform load-only operations")

        table_stats, load_time, _per_table_timings = adapter.load_data(benchmark, connection, data_dir)

        if validation_opts.enable_postload_validation:
            if hasattr(benchmark, "validate_loaded_data"):
                postload_result = benchmark.validate_loaded_data(connection, benchmark_name=benchmark_config.name)
            else:
                from benchbox.core.validation import DatabaseValidationEngine

                engine = DatabaseValidationEngine()
                postload_result = engine.validate_loaded_data(
                    connection,
                    benchmark_config.name.lower(),
                    benchmark_config.scale_factor,
                )

        phases = {
            "data_generation": {"status": "COMPLETED"},
            "schema_creation": {
                "status": "COMPLETED",
                "duration_seconds": schema_time,
            },
            "data_loading": {
                "status": "COMPLETED",
                "tables": table_stats,
                "duration_seconds": load_time,
            },
        }

        # Calculate total rows and data size
        total_rows = sum(table_stats.values()) if table_stats else 0
        # Try to calculate data size from benchmark output_dir if available
        data_size_mb = 0.0
        if hasattr(benchmark, "output_dir") and hasattr(adapter, "_calculate_data_size"):
            try:
                data_size_mb = adapter._calculate_data_size(benchmark.output_dir)
            except Exception:
                data_size_mb = 0.0

        result_obj = benchmark.create_enhanced_benchmark_result(
            platform=getattr(adapter, "platform_name", "load_only"),
            query_results=[],
            duration_seconds=schema_time + load_time,
            phases=phases,
            execution_metadata={"mode": "load_only"},
            schema_creation_time=schema_time,
            data_loading_time=load_time,
            table_statistics=table_stats,
            total_rows_loaded=total_rows,
            data_size_mb=data_size_mb,
        )
        return result_obj, postload_result
    finally:
        if connection is not None:
            adapter.close_connection(connection)


def _get_table_schemas_from_benchmark(benchmark: Any) -> dict[str, dict[str, Any]]:
    """Extract table schemas from benchmark instance.

    Args:
        benchmark: Benchmark instance with get_schema() method

    Returns:
        Dictionary mapping table_name → schema dict with {"name": ..., "columns": [...]}

    Raises:
        RuntimeError: If schema cannot be extracted
    """
    if not hasattr(benchmark, "get_schema"):
        raise RuntimeError(f"Benchmark {type(benchmark).__name__} does not provide get_schema() method")

    try:
        schema = benchmark.get_schema()
    except Exception as e:
        raise RuntimeError(f"Failed to get schema from benchmark: {e}") from e

    # Handle both dict and list return formats
    if isinstance(schema, dict):
        # Already in expected format: {table_name: {name, columns}}
        return schema
    elif isinstance(schema, list):
        # Convert list format to dict: [{name, columns}, ...] → {name: {name, columns}}
        return {table["name"].lower(): table for table in schema}
    else:
        raise RuntimeError(f"Unexpected schema format: {type(schema)}")


def _run_format_conversion(
    benchmark: Any,
    benchmark_config: BenchmarkConfig,
) -> dict[str, Any] | None:
    """Run format conversion after data generation.

    Args:
        benchmark: Benchmark instance with output_dir and get_schema()
        benchmark_config: Benchmark configuration with conversion settings in options

    Returns:
        Dictionary of conversion results by table name, or None if no conversion
    """
    # Extract conversion settings from benchmark_config.options
    options_dict = getattr(benchmark_config, "options", {}) or {}
    convert_format = options_dict.get("convert_format")

    # Check if conversion is requested
    if not convert_format:
        return None

    # Validate format
    allowed_formats = {"parquet", "delta", "iceberg"}
    if convert_format.lower() not in allowed_formats:
        logger.error(f"Invalid format: {convert_format}. Allowed: {allowed_formats}")
        return None

    output_dir = getattr(benchmark, "output_dir", None)
    if output_dir is None:
        logger.warning("Cannot convert format: benchmark output_dir not configured")
        return None

    manifest_path = output_dir / "_datagen_manifest.json"
    if not manifest_path.exists():
        logger.warning(f"Cannot convert format: manifest not found at {manifest_path}")
        return None

    # Get table schemas from benchmark
    try:
        schemas = _get_table_schemas_from_benchmark(benchmark)
    except RuntimeError as e:
        logger.error(f"Format conversion failed: {e}")
        return None

    # Build conversion options from benchmark_config.options
    conversion_compression = options_dict.get("conversion_compression", "snappy")
    conversion_partition_cols = options_dict.get("conversion_partition_cols", [])

    options = ConversionOptions(
        compression=conversion_compression,
        partition_cols=conversion_partition_cols or [],
        merge_shards=True,
        validate_row_count=True,
    )

    # Run conversion orchestration
    orchestrator = FormatConversionOrchestrator()
    logger.info(f"Converting benchmark data to {convert_format} format (compression: {options.compression})")

    try:
        results = orchestrator.convert_benchmark_tables(
            manifest_path=manifest_path,
            output_dir=output_dir,
            target_format=convert_format,
            schemas=schemas,
            options=options,
        )
        logger.info(f"✓ Format conversion complete: {len(results)} tables converted")
        return results
    except Exception as e:
        logger.error(f"Format conversion failed: {e}")
        raise RuntimeError(f"Format conversion to {convert_format} failed") from e


@dataclass
class LifecyclePhases:
    generate: bool = True
    load: bool = True
    execute: bool = True


@dataclass
class ValidationOptions:
    enable_preflight_validation: bool = False
    enable_postgen_manifest_validation: bool = False
    enable_postload_validation: bool = False


def run_benchmark_lifecycle(
    benchmark_config: BenchmarkConfig,
    database_config: DatabaseConfig | None,
    system_profile: SystemProfile | None,
    *,
    platform_config: dict[str, Any] | None = None,
    platform_adapter: Any | None = None,
    phases: LifecyclePhases | None = None,
    validation_opts: ValidationOptions | None = None,
    output_root: str | None = None,
    benchmark_instance: Any | None = None,
    verbosity: VerbositySettings | None = None,
    monitor: PerformanceMonitor | None = None,
    enable_resource_monitoring: bool = True,
) -> BenchmarkResults:
    """Run the complete benchmark lifecycle in core, returning BenchmarkResults.

    Args:
        benchmark_config: Core benchmark configuration
        database_config: Database configuration (None for data_only)
        system_profile: System profile used for benchmark instantiation
        platform_config: Platform adapter configuration (connection params, etc.)
        phases: Which lifecycle phases to execute
        validation_opts: Validation flags for pre/post generation and postload
        output_root: Optional output directory/URI for data generation
        benchmark_instance: Optional pre-constructed benchmark instance to use
        monitor: Optional PerformanceMonitor to track metrics. If None and monitoring
            not explicitly disabled, a default monitor will be created.
        enable_resource_monitoring: Whether to track CPU/memory during execution (default: True)

    Returns:
        BenchmarkResults representing the full execution with performance metrics attached
    """
    phases = phases or LifecyclePhases()
    validation_opts = validation_opts or ValidationOptions()

    # Create default monitor if not provided (deep integration - automatic monitoring)
    if monitor is None:
        monitor = PerformanceMonitor()

    # Start resource monitoring if enabled
    resource_monitor: ResourceMonitor | None = None
    if enable_resource_monitoring and monitor is not None:
        resource_monitor = ResourceMonitor(monitor, sample_interval=2.0)
        resource_monitor.start()

    # Record benchmark metadata in monitor
    if monitor is not None:
        monitor.set_metadata("benchmark", benchmark_config.name)
        monitor.set_metadata("scale_factor", getattr(benchmark_config, "scale_factor", 1.0))
        monitor.set_metadata("platform", database_config.type if database_config else "data_only")

    options_map = getattr(benchmark_config, "options", {}) or {}
    if verbosity is not None:
        verbosity_settings = verbosity
    else:
        stored_settings = options_map.get("verbosity_settings") if isinstance(options_map, Mapping) else None
        if isinstance(stored_settings, VerbositySettings):
            verbosity_settings = stored_settings
        elif isinstance(stored_settings, Mapping):
            verbosity_settings = VerbositySettings.from_mapping(stored_settings)
        else:
            verbosity_settings = VerbositySettings.from_mapping(options_map)

    benchmark = benchmark_instance or get_benchmark_instance(benchmark_config, system_profile)
    if isinstance(benchmark, VerbosityMixin):  # type: ignore[arg-type]
        benchmark.apply_verbosity(verbosity_settings)

    output_dir_handler = _resolve_output_dir_handler(benchmark, output_root)
    validation_records: list[tuple[str, ValidationResult]] = []

    # Determine test type and whether data is needed
    test_type = getattr(benchmark_config, "test_execution_type", "standard")
    needs_data = test_type != "data_only"

    # Ensure data exists for execution/loading (via reuse or generation)
    if needs_data:
        # Only run preflight validation when explicitly generating fresh data
        if phases.generate and validation_opts.enable_preflight_validation:
            preflight_result = _run_preflight_validation(benchmark, benchmark_config, output_dir_handler)
            validation_records.append(("preflight", preflight_result))
            if not preflight_result.is_valid:
                error_msg = ", ".join(preflight_result.errors) or "Unknown preflight validation error"
                raise RuntimeError(f"Preflight validation failed: {error_msg}")

        # Always ensure data exists (will reuse manifest if valid, or generate if needed)
        if monitor is not None:
            with monitor.time_operation("data_generation"):
                data_was_generated = _ensure_data_generated(benchmark, benchmark_config)
                if data_was_generated:
                    monitor.increment_counter("tables_generated", len(getattr(benchmark, "tables", []) or []))
        else:
            _ensure_data_generated(benchmark, benchmark_config)

        if validation_opts.enable_postgen_manifest_validation:
            manifest_result = _run_manifest_validation(benchmark, benchmark_config)
            validation_records.append(("post_generation_manifest", manifest_result))

        # Run format conversion if requested (only when data generation phase is enabled)
        if phases.generate:
            _run_format_conversion(benchmark, benchmark_config)

    if test_type == "data_only":
        result_obj = benchmark.create_enhanced_benchmark_result(
            platform="data_only",
            query_results=[],
            duration_seconds=0.0,
            phases={"data_generation": {"status": "COMPLETED"}},
            execution_metadata={"mode": "data_only"},
        )
        return _finalize_validation_metadata(result_obj, validation_records)

    adapter = platform_adapter
    if adapter is None and database_config is not None:
        adapter = get_platform_adapter(database_config.type, **(platform_config or {}))

    # Set benchmark instance on adapter for database validation
    # This allows the adapter to validate schema compatibility when checking
    # if an existing database can be reused
    if adapter is not None and benchmark:
        adapter.benchmark_instance = benchmark
        adapter.scale_factor = benchmark_config.scale_factor

    if adapter is not None and validation_opts.enable_postload_validation and hasattr(adapter, "enable_validation"):
        adapter.enable_validation = True

    if adapter is not None and isinstance(adapter, VerbosityMixin):
        adapter.apply_verbosity(verbosity_settings)

    # Check if we should run load-only mode:
    # - Explicit test_type == "load_only" OR
    # - Phases indicate load without execute (e.g., --phases generate,load)
    should_run_load_only = test_type == "load_only" or (phases.load and not phases.execute and adapter is not None)

    if should_run_load_only:
        if adapter is None:
            raise RuntimeError("Load-only mode requires a platform adapter and database configuration")

        result_obj, postload_result = _execute_load_only_mode(
            benchmark=benchmark,
            benchmark_config=benchmark_config,
            adapter=adapter,
            platform_config=platform_config,
            validation_opts=validation_opts,
        )

        if postload_result is not None:
            validation_records.append(("post_load", postload_result))

        return _finalize_validation_metadata(result_obj, validation_records)

    # Only return early for setup-only scenarios (no loading, no execution)
    # This prevents skipping schema creation when phases.load=True
    if adapter is None or (not phases.execute and not phases.load):
        # If execute phase is requested but adapter is missing, this is a configuration error
        if phases.execute and adapter is None:
            raise RuntimeError(
                "Cannot execute benchmark: platform adapter not initialized. "
                "This indicates database configuration is missing or adapter creation failed. "
                "Ensure --platform parameter is provided when using execution phases (power/throughput/maintenance)."
            )

        result_obj = benchmark.create_enhanced_benchmark_result(
            platform=(adapter.platform_name if adapter else "unknown"),
            query_results=[],
            duration_seconds=0.0,
            phases={"setup": {"status": "COMPLETED"}},
            execution_metadata={"mode": "setup_only"},
        )
        return _finalize_validation_metadata(result_obj, validation_records)

    options = getattr(benchmark_config, "options", {}) or {}
    iterations = int(
        options.get("power_iterations", GENERIC_POWER_DEFAULT_MEASUREMENT_ITERATIONS)
        or GENERIC_POWER_DEFAULT_MEASUREMENT_ITERATIONS
    )
    warmups = int(
        options.get("power_warmup_iterations", GENERIC_POWER_DEFAULT_WARMUP_ITERATIONS)
        or GENERIC_POWER_DEFAULT_WARMUP_ITERATIONS
    )
    fail_fast = bool(options.get("power_fail_fast", False))

    run_config = RunConfig(
        query_subset=benchmark_config.queries,
        concurrent_streams=benchmark_config.concurrency,
        test_execution_type=test_type,
        scale_factor=benchmark_config.scale_factor,
        seed=(int(options.get("seed")) if options.get("seed") is not None else None),
        connection={
            "database_path": (platform_config or {}).get("database_path"),
        },
        enable_postload_validation=validation_opts.enable_postload_validation,
        verbose=verbosity_settings.verbose,
        verbose_level=verbosity_settings.level,
        verbose_enabled=verbosity_settings.verbose_enabled,
        very_verbose=verbosity_settings.very_verbose,
        quiet=verbosity_settings.quiet,
        iterations=max(1, iterations),
        warm_up_iterations=max(0, warmups),
        power_fail_fast=fail_fast,
    )

    kwargs = {k: v for k, v in run_config.__dict__.items() if k != "benchmark"}
    result_obj = adapter.run_benchmark(benchmark, **kwargs)

    if validation_opts.enable_postload_validation:
        postload_result = _run_postload_validation(adapter, benchmark_config, platform_config)
        if postload_result is not None:
            validation_records.append(("post_load", postload_result))

    # Stop resource monitoring and attach performance snapshot to results
    if resource_monitor is not None:
        resource_monitor.stop()

    result_with_validation = _finalize_validation_metadata(result_obj, validation_records)

    # Attach performance monitoring snapshot to result
    if monitor is not None:
        snapshot = monitor.snapshot()
        attach_snapshot_to_result(result_with_validation, snapshot)

    return result_with_validation


def _ensure_data_generated(benchmark: Any, config: BenchmarkConfig) -> bool:
    """Ensure data is generated, respecting manifest and generator validator.

    Implements the idempotent behavior: reuse valid existing data when possible,
    unless force_regenerate is requested; optionally fail when no_regenerate is set
    and data is missing/invalid.

    Returns:
        True if data was freshly generated, False if reused from existing manifest
    """
    options = getattr(config, "options", {}) or {}
    force_regenerate_flag = bool(options.get("force_regenerate"))
    no_regenerate_flag = bool(options.get("no_regenerate"))

    # If tables already present, assume generation done
    if getattr(benchmark, "tables", None):
        return False

    output_dir = getattr(benchmark, "output_dir", None)
    manifest_valid = False
    manifest_found = False
    manifest_data: dict | None = None

    if output_dir and not force_regenerate_flag:
        manifest_valid, manifest_data, manifest_found = _validate_manifest_if_present(benchmark, config)
        if manifest_valid:
            summary = _populate_tables_from_manifest(benchmark, manifest_data)
            if summary:
                _emit_manifest_reuse_message(benchmark, summary)

            # Allow benchmarks to ensure auxiliary files exist even when reusing data
            # This is needed for benchmarks that generate additional test files beyond the main data
            ensure_auxiliary = getattr(benchmark, "ensure_auxiliary_data_files", None)
            if callable(ensure_auxiliary):
                try:
                    ensure_auxiliary()
                except (OSError, PermissionError) as e:
                    # Critical system errors - re-raise (disk full, permissions, I/O failure)
                    raise RuntimeError(
                        f"Failed to generate auxiliary data files due to system error: {e}. "
                        "Check disk space, permissions, and file system health."
                    ) from e
                except ImportError as e:
                    # Missing optional dependency - log warning and continue
                    logger = logging.getLogger("benchbox.core.runner")
                    logger.warning(
                        f"Failed to generate auxiliary data files due to missing dependency: {e}. "
                        "Some benchmark operations may not be available."
                    )
                except Exception as e:
                    # Other errors - log warning but continue (auxiliary files may not be critical)
                    logger = logging.getLogger("benchbox.core.runner")
                    logger.warning(
                        f"Failed to generate auxiliary data files: {type(e).__name__}: {e}. "
                        "Some benchmark operations may fail."
                    )

            return False

    if no_regenerate_flag and not force_regenerate_flag:
        reason = "manifest is invalid or stale" if manifest_found else "manifest is missing"
        raise RuntimeError(f"no_regenerate is set but {reason}")

    # If manifest existed but failed validation, emit a debug message before regenerating
    if manifest_found and not manifest_valid:
        try:
            log_method = getattr(benchmark, "log_verbose", None)
            message = "⚠️ Manifest validation failed; regenerating benchmark data"
            if callable(log_method):
                log_method(message)
            else:
                print(message)
        except Exception:  # pragma: no cover - logging should not block generation
            pass

    # Perform generation (force_regenerate_flag is respected by skipping reuse)
    benchmark.generate_data()
    return True


def _populate_tables_from_manifest(benchmark: Any, manifest: dict | None = None) -> dict[str, Any] | None:
    """Populate benchmark.tables from _datagen_manifest.json when available.

    Returns a summary dictionary with table_count, file_count, created_at when manifest
    data is loaded successfully. Returns None when manifest is missing or invalid.
    """
    try:
        from benchbox.utils.datagen_manifest import get_table_files

        output_dir = getattr(benchmark, "output_dir", None)
        if not output_dir:
            return None
        manifest_data = manifest
        manifest_path = output_dir.joinpath("_datagen_manifest.json")
        if manifest_data is None:
            if not hasattr(manifest_path, "exists") or not manifest_path.exists():
                return None
            with manifest_path.open("r") as f:
                manifest_data = json.load(f)
        if manifest_data is None:
            return None
        tbl_map: dict[str, Any] = {}
        total_files = 0
        for table in (manifest_data.get("tables") or {}).keys():
            entries = get_table_files(manifest_data, table)
            if not entries:
                continue
            total_files += len(entries)
            paths = [output_dir.joinpath(e.get("path")) for e in entries if e.get("path")]
            if not paths:
                continue
            tbl_map[table] = paths if len(paths) > 1 else paths[0]
        if tbl_map:
            benchmark.tables = tbl_map
        return {
            "table_count": len(tbl_map),
            "file_count": total_files,
            "created_at": manifest_data.get("created_at"),
        }
    except Exception:
        # Non-fatal; leave tables as-is
        return None


def _emit_manifest_reuse_message(benchmark: Any, summary: dict[str, Any]) -> None:
    """Emit a concise console/log message when data reuse is triggered."""

    created_at = summary.get("created_at")
    table_count = summary.get("table_count", 0)
    file_count = summary.get("file_count", 0)
    timestamp = f"created {created_at}" if created_at else "existing manifest"
    table_label = "table" if table_count == 1 else "tables"
    file_label = "file" if file_count == 1 else "files"
    message = f"🔄 Reusing benchmark data ({timestamp}; {table_count} {table_label}, {file_count} {file_label})"

    # Prefer benchmark verbosity-aware logging when available
    log_method = getattr(benchmark, "log_verbose", None)
    if callable(log_method):
        try:
            log_method(message)
            return
        except Exception:
            pass

    # Fall back to standard output if verbose logging is not available
    with contextlib.suppress(Exception):
        print(message)


def _validate_manifest_if_present(benchmark: Any, config: BenchmarkConfig) -> tuple[bool, dict | None, bool]:
    """Validate manifest structure and referenced files.

    Returns (valid, manifest_dict or None). Non-fatal; failures are signaled by return value.
    """
    try:
        output_dir = getattr(benchmark, "output_dir", None)
        if not output_dir:
            return False, None, False
        manifest_path = output_dir.joinpath("_datagen_manifest.json")
        manifest_found = bool(hasattr(manifest_path, "exists") and manifest_path.exists())
        if not manifest_found:
            return False, None, False
        with manifest_path.open("r") as f:
            manifest = json.load(f)

        manifest_benchmark = str(manifest.get("benchmark", "")).lower()
        allowed_names = _resolve_manifest_allowed_names(benchmark, config)
        if manifest_benchmark not in allowed_names:
            return False, None, True

        if float(manifest.get("scale_factor", -1)) != float(config.scale_factor):
            return False, None, True

        # Validate manifest entries using V2-aware helper
        from benchbox.utils.datagen_manifest import get_table_files

        tables = manifest.get("tables", {}) or {}
        for table_name in tables.keys():
            entries = get_table_files(manifest, table_name)
            for entry in entries:
                rel = entry.get("path")
                size = int(entry.get("size_bytes", -1))
                if rel is None or size < 0:
                    return False, None, True
                fp = output_dir.joinpath(rel)
                if (not hasattr(fp, "exists") or not fp.exists()) or (
                    not hasattr(fp, "stat") or fp.stat().st_size != size
                ):
                    return False, None, True
        return True, manifest, True
    except Exception:
        return False, None, bool(locals().get("manifest_found", False))
