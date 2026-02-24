"""Primary adapter for ClickHouse platforms."""

from __future__ import annotations

import logging

from benchbox.platforms.base import DriverIsolationCapability, PlatformAdapter
from benchbox.utils.dependencies import check_platform_dependencies, get_dependency_error_message

from .diagnostics import ClickHouseDiagnosticsMixin
from .metadata import ClickHouseMetadataMixin
from .setup import ClickHouseSetupMixin
from .tuning import ClickHouseTuningMixin
from .workload import ClickHouseWorkloadMixin

logger = logging.getLogger(__name__)


class ClickHouseAdapter(
    ClickHouseMetadataMixin,
    ClickHouseSetupMixin,
    ClickHouseDiagnosticsMixin,
    ClickHouseWorkloadMixin,
    ClickHouseTuningMixin,
    PlatformAdapter,
):
    """High-level adapter coordinating ClickHouse operations.

    Known Limitations:
        TPC-DS queries with known incompatibilities:
        - Query 14: INTERSECT DISTINCT requires manual alias addition
        - Query 30: Query plan cloning not implemented for aggregation steps (Code: 48)
        - Query 66: Nested aggregation not supported - requires query rewrite
        - Query 81: Query plan cloning not implemented for aggregation steps (Code: 48)

        These queries may fail even with transformations applied and require
        manual query rewriting or ClickHouse engine improvements.
    """

    driver_isolation_capability = DriverIsolationCapability.NOT_FEASIBLE

    # Known incompatible queries that may fail despite transformations
    KNOWN_INCOMPATIBLE_QUERIES = {
        "tpcds": [14, 30, 66, 81],
    }

    def __init__(self, **config):
        super().__init__(**config)

        self._dialect = "clickhouse"

        # Determine deployment mode (from factory via colon syntax: clickhouse:local).
        # Default to local mode for easiest onboarding (no credentials required).
        deployment_mode = config.get("deployment_mode")
        self.deployment_mode = deployment_mode.lower() if deployment_mode else "local"

        # Validate deployment mode
        # Note: "cloud" mode is now a separate first-class platform: clickhouse-cloud
        # The _is_cloud_subclass flag is set by ClickHouseCloudAdapter to bypass this check
        valid_modes = {"local", "server"}
        is_cloud_subclass = config.get("_is_cloud_subclass", False)
        if self.deployment_mode == "cloud" and not is_cloud_subclass:
            raise ValueError(
                "ClickHouse Cloud is now a separate first-class platform.\n"
                "Use --platform clickhouse-cloud instead of --platform clickhouse:cloud\n"
                "For more information: benchbox run --platform clickhouse-cloud --help"
            )
        # Cloud mode is valid when called from ClickHouseCloudAdapter
        if is_cloud_subclass:
            valid_modes = {"local", "server", "cloud"}
        if self.deployment_mode not in valid_modes:
            raise ValueError(
                f"Invalid ClickHouse deployment mode '{self.deployment_mode}'. "
                f"Valid modes: {', '.join(sorted(valid_modes))}"
            )

        # Mode-specific validation and setup
        if self.deployment_mode == "server":
            available, missing = check_platform_dependencies("clickhouse", ["clickhouse-driver"])
            if not available:
                error_msg = get_dependency_error_message("clickhouse", missing)
                raise ImportError(error_msg)
            self._setup_server_mode(config)
        elif self.deployment_mode == "local":
            import importlib.util

            if importlib.util.find_spec("chdb") is None:
                raise ImportError(
                    "ClickHouse local mode requires chDB but it is not installed.\n"
                    "To resolve this issue:\n"
                    "  1. Install chDB: uv add chdb\n"
                    "  2. Or switch to server mode: --platform clickhouse:server\n"
                    "  3. Or use ClickHouse Cloud: --platform clickhouse-cloud\n"
                    "  4. Or use a different platform (e.g., DuckDB)\n"
                    "\nFor more information about chDB, visit: https://github.com/chdb-io/chdb"
                )
            self._setup_local_mode(config)
        elif self.deployment_mode == "cloud":
            # Cloud mode is only valid when called from ClickHouseCloudAdapter
            # Check for clickhouse-connect dependency
            import importlib.util

            if importlib.util.find_spec("clickhouse_connect") is None:
                raise ImportError(
                    "ClickHouse Cloud requires clickhouse-connect but it is not installed.\n"
                    "To resolve this issue:\n"
                    "  1. Install clickhouse-connect: uv add clickhouse-connect\n"
                    "  2. Or use local mode: --platform clickhouse:local\n"
                    "\nFor more information, visit: https://clickhouse.com/docs/en/integrations/python"
                )
            self._setup_cloud_mode(config)


__all__ = ["ClickHouseAdapter"]
