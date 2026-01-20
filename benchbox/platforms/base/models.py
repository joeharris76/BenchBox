"""Dataclasses describing platform adapter configuration and results."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

try:  # Optional import for type checking without runtime requirement
    from benchbox.core.tuning.interface import UnifiedTuningConfiguration
except ImportError:  # pragma: no cover - fallback for minimal installs
    UnifiedTuningConfiguration = None  # type: ignore


@dataclass
class ConnectionConfig:
    """Connection configuration for database platforms."""

    host: str | None = None
    port: int | None = None
    database: str | None = None
    username: str | None = None
    password: str | None = None
    connection_string: str | None = None

    auth_method: str = "password"
    token: str | None = None
    service_account_path: str | None = None

    ssl_enabled: bool = False
    ssl_cert_path: str | None = None
    ssl_key_path: str | None = None
    ssl_ca_path: str | None = None

    pool_size: int = 5
    max_overflow: int = 10
    pool_timeout: int = 30

    extra_params: dict[str, Any] = field(default_factory=dict)

    tuning_enabled: bool = False
    unified_tuning_configuration: UnifiedTuningConfiguration | None = None

    def get_env_value(self, key: str, default: Any = None) -> Any:
        value = getattr(self, key, default)
        if isinstance(value, str) and value.startswith("${") and value.endswith("}"):
            import os

            env_var = value[2:-1]
            return os.getenv(env_var, default)
        return value


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
class DatabaseValidationResult:
    """Result of compatibility checks for an existing database."""

    is_valid: bool
    can_reuse: bool
    issues: list[str]
    warnings: list[str]
    tuning_valid: bool | None = None
    tables_valid: bool | None = None
    row_counts_valid: bool | None = None


__all__ = [
    "ConnectionConfig",
    "TableGenerationStats",
    "DataGenerationPhase",
    "TableCreationStats",
    "SchemaCreationPhase",
    "TableLoadingStats",
    "DataLoadingPhase",
    "ValidationPhase",
    "SetupPhase",
    "QueryExecution",
    "PowerTestPhase",
    "ThroughputStream",
    "ThroughputTestPhase",
    "MaintenanceOperation",
    "MaintenanceTestPhase",
    "DatabaseValidationResult",
]
