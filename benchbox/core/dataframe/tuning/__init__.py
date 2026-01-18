"""DataFrame tuning configuration module.

This module provides configuration classes for tuning DataFrame adapter
runtime behavior, including parallelism, memory management, execution mode,
data type optimization, I/O settings, and GPU acceleration.

Unlike SQL tuning (which affects DDL/schema), DataFrame tuning affects
runtime execution parameters and is applied when the adapter is initialized.

Example:
    >>> from benchbox.core.dataframe.tuning import (
    ...     DataFrameTuningConfiguration,
    ...     ParallelismConfiguration,
    ...     ExecutionConfiguration,
    ... )
    >>>
    >>> # Create a custom configuration
    >>> config = DataFrameTuningConfiguration(
    ...     parallelism=ParallelismConfiguration(thread_count=8),
    ...     execution=ExecutionConfiguration(streaming_mode=True),
    ... )
    >>>
    >>> # Check which settings are enabled
    >>> enabled = config.get_enabled_settings()
    >>> print([t.value for t in enabled])
    ['thread_count', 'streaming_mode']

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

from benchbox.core.dataframe.tuning.defaults import (
    SystemProfile,
    detect_system_profile,
    get_profile_summary,
    get_smart_defaults,
)
from benchbox.core.dataframe.tuning.interface import (
    DataFrameTuningConfiguration,
    DataTypeConfiguration,
    ExecutionConfiguration,
    GPUConfiguration,
    IOConfiguration,
    MemoryConfiguration,
    ParallelismConfiguration,
    TuningMetadata,
)
from benchbox.core.dataframe.tuning.loader import (
    DataFrameTuningLoader,
    DataFrameTuningLoadError,
    DataFrameTuningSaveError,
    load_dataframe_tuning,
    save_dataframe_tuning,
)
from benchbox.core.dataframe.tuning.types import (
    DataFrameTuningType,
    get_all_platforms,
)
from benchbox.core.dataframe.tuning.validation import (
    TuningValidationError,
    ValidationIssue,
    ValidationLevel,
    format_issues,
    has_errors,
    has_warnings,
    validate_dataframe_tuning,
)
from benchbox.core.dataframe.tuning.write_config import (
    DataFrameWriteConfiguration,
    DataFrameWriteTuningType,
    PartitionColumn,
    PartitionStrategy,
    SortColumn,
    get_platform_write_capabilities,
    validate_write_config_for_platform,
)

__all__ = [
    # Main configuration class
    "DataFrameTuningConfiguration",
    # Sub-configuration classes
    "ParallelismConfiguration",
    "MemoryConfiguration",
    "ExecutionConfiguration",
    "DataTypeConfiguration",
    "IOConfiguration",
    "GPUConfiguration",
    "TuningMetadata",
    # Loader
    "DataFrameTuningLoader",
    "DataFrameTuningLoadError",
    "DataFrameTuningSaveError",
    "load_dataframe_tuning",
    "save_dataframe_tuning",
    # Validation
    "validate_dataframe_tuning",
    "ValidationIssue",
    "ValidationLevel",
    "TuningValidationError",
    "has_errors",
    "has_warnings",
    "format_issues",
    # Defaults
    "get_smart_defaults",
    "detect_system_profile",
    "get_profile_summary",
    "SystemProfile",
    # Enum
    "DataFrameTuningType",
    # Utilities
    "get_all_platforms",
    # Write configuration (physical layout)
    "DataFrameWriteConfiguration",
    "DataFrameWriteTuningType",
    "PartitionColumn",
    "PartitionStrategy",
    "SortColumn",
    "get_platform_write_capabilities",
    "validate_write_config_for_platform",
]
