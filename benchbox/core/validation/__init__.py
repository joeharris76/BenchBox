"""Data validation module for BenchBox benchmarks.

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

from .data import DataValidator, RowCountDiscrepancy, ValidationResult as DataValidationResult
from .engines import (
    BenchmarkExpectations,
    DatabaseValidationEngine,
    DataValidationEngine,
    ValidationResult,
    ValidationSummary,
)
from .service import PlatformValidationResult, ValidationService

__all__ = [
    "DataValidator",
    "ValidationResult",  # New core validation result
    "DataValidationResult",  # Legacy data validation result
    "RowCountDiscrepancy",
    "ValidationSummary",
    "BenchmarkExpectations",
    "DataValidationEngine",
    "DatabaseValidationEngine",
    "PlatformValidationResult",
    "ValidationService",
]
