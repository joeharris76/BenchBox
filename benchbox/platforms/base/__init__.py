"""Base platform adapter package."""

from __future__ import annotations

from .adapter import (
    BenchmarkResults,
    EnhancedBenchmarkResults,
    ExecutionPhases,
    PlatformAdapter,
    PlatformAdapterConnection,
    PlatformAdapterCursor,
    QueryDefinition,
)
from .models import (
    ConnectionConfig,
    DatabaseValidationResult,
    DataGenerationPhase,
    MaintenanceOperation,
    MaintenanceTestPhase,
    PowerTestPhase,
    QueryExecution,
    SchemaCreationPhase,
    SetupPhase,
    TableCreationStats,
    TableGenerationStats,
    TableLoadingStats,
    ThroughputStream,
    ThroughputTestPhase,
    ValidationPhase,
)
from .utils import FileFormatInfo, detect_file_format, is_non_interactive

__all__ = [
    "ConnectionConfig",
    "DatabaseValidationResult",
    "DataGenerationPhase",
    "BenchmarkResults",
    "EnhancedBenchmarkResults",
    "ExecutionPhases",
    "FileFormatInfo",
    "MaintenanceOperation",
    "MaintenanceTestPhase",
    "PlatformAdapter",
    "PlatformAdapterConnection",
    "PlatformAdapterCursor",
    "PowerTestPhase",
    "QueryExecution",
    "SchemaCreationPhase",
    "SetupPhase",
    "TableCreationStats",
    "TableGenerationStats",
    "TableLoadingStats",
    "ThroughputStream",
    "ThroughputTestPhase",
    "ValidationPhase",
    "QueryDefinition",
    "detect_file_format",
    "is_non_interactive",
]
