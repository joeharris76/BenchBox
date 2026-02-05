"""Shared logging helpers for validation workflows."""

from __future__ import annotations

import logging
from collections.abc import Iterable

from .models import RowCountDiscrepancy

logger = logging.getLogger(__name__)


def log_row_count_summary(result, *, log=logger) -> None:
    """Log a consistent summary for row count validation results."""
    summary = result.get_summary()

    if result.is_valid:
        log.info(
            "Row count validation PASSED: %s/%s tables",
            summary["passed_tables"],
            summary["total_tables"],
        )
    else:
        log.warning(
            "Row count validation FAILED: %s tables failed, %s passed, %s warnings",
            summary["failed_tables"],
            summary["passed_tables"],
            summary["warning_tables"],
        )

    _log_significant_discrepancies(result.get_significant_discrepancies(), log=log)


def _log_significant_discrepancies(discrepancies: Iterable[RowCountDiscrepancy], *, log=logger) -> None:
    """Emit log entries for the discrepancies that exceeded tolerances."""
    discrepancies = list(discrepancies)
    if not discrepancies:
        return

    log.warning("Found %s significant discrepancies:", len(discrepancies))
    for disc in discrepancies:
        log.warning("  %s", disc)


__all__ = ["log_row_count_summary"]
