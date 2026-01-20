"""Core tuning interface classes for BenchBox.

This module provides the core tuning interface classes that define how database
table tunings are configured, validated, and applied across different platforms.

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

from .ddl_generator import (
    BaseDDLGenerator,
    ColumnDefinition,
    ColumnNullability,
    DDLGenerator,
    NoOpDDLGenerator,
    TuningClauses,
)
from .interface import (
    BenchmarkTunings,
    ClusteringConfig,
    PartitioningConfig,
    SortKeyConfig,
    TableTuning,
    TuningColumn,
    TuningType,
)
from .metadata import (
    MetadataValidationResult,
    TuningMetadata,
    TuningMetadataManager,
)
from .validation import (
    ValidationIssue,
    ValidationLevel,
    ValidationResult,
    detect_tuning_conflicts,
    validate_benchmark_tunings,
    validate_column_types,
    validate_columns_exist,
    validate_constraint_consistency,
)

__all__ = [
    # DDL Generator Protocol
    "DDLGenerator",
    "BaseDDLGenerator",
    "NoOpDDLGenerator",
    "TuningClauses",
    "ColumnDefinition",
    "ColumnNullability",
    # Tuning Interface
    "TuningType",
    "TuningColumn",
    "TableTuning",
    "BenchmarkTunings",
    # Advanced Tuning Configuration
    "PartitioningConfig",
    "SortKeyConfig",
    "ClusteringConfig",
    # Validation
    "ValidationLevel",
    "ValidationIssue",
    "ValidationResult",
    "validate_columns_exist",
    "validate_column_types",
    "detect_tuning_conflicts",
    "validate_benchmark_tunings",
    "validate_constraint_consistency",
    # Metadata
    "TuningMetadata",
    "TuningMetadataManager",
    "MetadataValidationResult",
]
