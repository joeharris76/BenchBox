"""
Core result models for benchmark execution.

These dataclasses capture detailed execution phases and summary metrics for
benchmarks and are intentionally free of CLI/platform imports to avoid cycles.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from benchbox.core.results.query_plan_models import QueryPlanDAG


@dataclass
class TableGenerationStats:
    generation_time_ms: int
    status: str
    rows_generated: int
    data_size_bytes: int
    file_path: str
    error_type: str | None = None
    error_message: str | None = None
    rows_attempted: int | None = None
    bytes_attempted: int | None = None
    error_timestamp: str | None = None


@dataclass
class DataGenerationPhase:
    duration_ms: int
    status: str
    tables_generated: int
    total_rows_generated: int
    total_data_size_bytes: int
    per_table_stats: dict[str, TableGenerationStats]


@dataclass
class TableCreationStats:
    creation_time_ms: int
    status: str
    constraints_applied: int
    indexes_created: int
    error_type: str | None = None
    error_message: str | None = None
    error_timestamp: str | None = None


@dataclass
class SchemaCreationPhase:
    duration_ms: int
    status: str
    tables_created: int
    constraints_applied: int
    indexes_created: int
    per_table_creation: dict[str, TableCreationStats]


@dataclass
class TableLoadingStats:
    rows: int
    load_time_ms: int
    status: str
    error_type: str | None = None
    error_message: str | None = None
    rows_processed: int | None = None
    rows_successful: int | None = None
    error_timestamp: str | None = None


@dataclass
class DataLoadingPhase:
    duration_ms: int
    status: str
    total_rows_loaded: int
    tables_loaded: int
    per_table_stats: dict[str, TableLoadingStats]


@dataclass
class ValidationPhase:
    duration_ms: int
    row_count_validation: str
    schema_validation: str
    data_integrity_checks: str
    validation_details: dict[str, Any] | None = None


@dataclass
class SetupPhase:
    data_generation: DataGenerationPhase | None = None
    schema_creation: SchemaCreationPhase | None = None
    data_loading: DataLoadingPhase | None = None
    validation: ValidationPhase | None = None


@dataclass
class QueryExecution:
    query_id: str
    stream_id: str
    execution_order: int
    execution_time_ms: int
    status: str
    rows_returned: int | None = None
    resource_usage: dict[str, Any] | None = None
    error_message: str | None = None
    iteration: int | None = None
    run_type: str | None = None
    # Row count validation - nested object structure
    row_count_validation: dict[str, Any] | None = None  # Contains: expected, actual, status, error/warning
    # Cost estimation
    cost: float | None = None  # Compute cost in USD for this query
    # Query plan capture (structured DAG representation)
    query_plan: QueryPlanDAG | None = None  # Captured query execution plan
    plan_fingerprint: str | None = None  # SHA256 hash for fast plan comparison
    plan_capture_time_ms: float | None = None  # Time spent capturing plan (EXPLAIN + parse)


@dataclass
class PowerTestPhase:
    start_time: str
    end_time: str
    duration_ms: int
    query_executions: list[QueryExecution]
    geometric_mean_time: float
    power_at_size: float


@dataclass
class ThroughputStream:
    stream_id: int
    start_time: str
    end_time: str
    duration_ms: int
    query_executions: list[QueryExecution]


@dataclass
class ThroughputTestPhase:
    start_time: str
    end_time: str
    duration_ms: int
    num_streams: int
    streams: list[ThroughputStream]
    total_queries_executed: int
    throughput_at_size: float


@dataclass
class MaintenanceOperation:
    operation: str
    operation_type: str
    table: str
    execution_time_ms: int
    rows_affected: int
    status: str
    error_message: str | None = None


@dataclass
class MaintenanceTestPhase:
    start_time: str
    end_time: str
    duration_ms: int
    maintenance_operations: list[MaintenanceOperation]
    query_executions: list[QueryExecution]


@dataclass
class ExecutionPhases:
    setup: SetupPhase
    power_test: PowerTestPhase | None = None
    throughput_test: ThroughputTestPhase | None = None
    maintenance_test: MaintenanceTestPhase | None = None


@dataclass
class QueryDefinition:
    sql: str
    parameters: dict[str, Any] | None = None


@dataclass
class BenchmarkResults:
    benchmark_name: str
    platform: str
    scale_factor: float
    execution_id: str
    timestamp: datetime
    duration_seconds: float
    total_queries: int
    successful_queries: int
    failed_queries: int
    # Summary of queries (flattened list for basic consumers)
    query_results: list[dict[str, Any]] = field(default_factory=list)
    # Summary metrics
    total_execution_time: float = 0.0
    average_query_time: float = 0.0
    # Setup metrics
    data_loading_time: float = 0.0
    schema_creation_time: float = 0.0
    total_rows_loaded: int = 0
    data_size_mb: float = 0.0
    table_statistics: dict[str, int] = field(default_factory=dict)
    # Optional detailed per-query timing info (for CSV export and analysis)
    per_query_timings: list[dict[str, Any]] | None = field(default_factory=list)
    # Optional detailed structures
    execution_phases: ExecutionPhases | None = None
    query_definitions: dict[str, dict[str, QueryDefinition]] | None = None
    # TPC metrics and execution type
    test_execution_type: str = "standard"
    power_at_size: float | None = None
    throughput_at_size: float | None = None
    qphh_at_size: float | None = None
    geometric_mean_execution_time: float | None = None
    # Validation and metadata
    validation_status: str = "PASSED"
    validation_details: dict[str, Any] | None = None
    platform_info: dict[str, Any] | None = None
    platform_metadata: dict[str, Any] | None = None
    tunings_applied: dict[str, Any] | None = None
    tuning_config_hash: str | None = None  # SHA-256 hash for config comparison
    tuning_source_file: str | None = None  # Path to tuning YAML file if applicable
    tuning_validation_status: str = "NOT_VALIDATED"
    tuning_metadata_saved: bool = False
    system_profile: dict[str, Any] | None = None
    database_name: str | None = None
    anonymous_machine_id: str | None = None
    execution_metadata: dict[str, Any] | None = None
    performance_characteristics: dict[str, Any] = field(default_factory=dict)
    performance_summary: dict[str, Any] = field(default_factory=dict)
    # Cost estimation
    cost_summary: dict[str, Any] | None = None  # Contains: total_cost, phase_costs, platform_details
    driver_package: str | None = None
    driver_version_requested: str | None = None
    driver_version_resolved: str | None = None
    driver_auto_install: bool = False
    # Additional optional attributes set dynamically
    output_filename: str | None = None
    resource_utilization: dict[str, Any] | None = None
    _benchmark_id_override: str | None = None
    summary_metrics: dict[str, Any] = field(default_factory=dict)
    query_subset: list[str] | None = None
    concurrency_level: int | None = None
    benchmark_version: str | None = None
    # Query plan capture statistics
    query_plans_captured: int = 0  # Count of queries with captured plans
    plan_capture_failures: int = 0  # Count of plan capture failures
    plan_capture_errors: list[dict[str, str]] = field(default_factory=list)
    plan_comparison_summary: dict[str, Any] | None = None  # Cross-run/platform plan comparison results
    # Query plan capture timing (set during result aggregation)
    total_plan_capture_time_ms: float = 0.0  # Total time spent on plan capture
    avg_plan_capture_overhead_pct: float = 0.0  # Average overhead as % of query time
    max_plan_capture_time_ms: float = 0.0  # Maximum single capture time

    @property
    def benchmark_id(self) -> str:
        """Return benchmark identifier derived from benchmark name."""
        override = getattr(self, "_benchmark_id_override", None)
        if override:
            return override

        if isinstance(self.execution_metadata, dict):
            metadata_override = self.execution_metadata.get("benchmark_id")
            if isinstance(metadata_override, str) and metadata_override:
                return metadata_override

        normalized = self.benchmark_name.lower().replace(" ", "_").replace("-", "_")
        while "__" in normalized:
            normalized = normalized.replace("__", "_")
        return normalized
