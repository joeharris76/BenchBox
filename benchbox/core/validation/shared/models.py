"""Shared validation data models."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class ValidationStatus(Enum):
    """Status of validation checks."""

    PASSED = "passed"
    FAILED = "failed"
    WARNING = "warning"
    SKIPPED = "skipped"


@dataclass
class RowCountDiscrepancy:
    """Represents a row count validation discrepancy."""

    table_name: str
    expected_count: int
    actual_count: int
    difference: int
    percentage_diff: float
    tolerance_exceeded: bool
    status: ValidationStatus

    @property
    def is_significant(self) -> bool:
        """True when the discrepancy exceeds the configured tolerance."""
        return self.tolerance_exceeded

    def to_summary(self) -> str:
        """Return a short human-readable summary line."""
        return (
            f"Table '{self.table_name}': expected {self.expected_count:,}, "
            f"actual {self.actual_count:,} ({self.percentage_diff:+.2f}%)"
        )

    def __str__(self) -> str:
        return self.to_summary()


__all__ = ["ValidationStatus", "RowCountDiscrepancy"]
