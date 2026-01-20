"""Expected query results for benchmark validation.

This module provides infrastructure for validating query execution by comparing
actual row counts against expected results. It supports:
- Scale-independent queries (same result count at any scale factor)
- Scale-dependent queries (result count varies with scale factor)
- Non-deterministic queries (result count within a range)
- Formula-based expectations (e.g., "number of regions is always 5")

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

import logging

logger = logging.getLogger(__name__)

# Track whether providers have been registered to avoid redundant logging
_providers_registered = False

# Import providers to register them with the global registry
from benchbox.core.expected_results import tpcds_results, tpch_results  # noqa: F401, E402
from benchbox.core.expected_results.registry import get_registry  # noqa: E402


def register_all_providers():
    """Explicitly register all expected results providers.

    This function is idempotent - safe to call multiple times.
    It ensures all benchmark providers are registered with the global registry.

    Called automatically when QueryValidator is instantiated, but can also
    be called explicitly if needed.
    """
    global _providers_registered

    # Verify registration and log status (only on first call)
    registry = get_registry()
    benchmarks = registry.list_available_benchmarks()

    if not _providers_registered:
        if benchmarks:
            logger.debug(f"Expected results providers registered for: {', '.join(benchmarks)}")
        else:
            logger.warning(
                "No expected results providers registered. "
                "Validation will be skipped for all benchmarks. "
                "This may indicate a module import issue."
            )
        _providers_registered = True

    return benchmarks
