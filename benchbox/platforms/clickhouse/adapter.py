"""Primary adapter for ClickHouse platforms."""

from __future__ import annotations

import logging

from benchbox.platforms.base import PlatformAdapter
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

    # Known incompatible queries that may fail despite transformations
    KNOWN_INCOMPATIBLE_QUERIES = {
        "tpcds": [14, 30, 66, 81],
    }

    def __init__(self, **config):
        super().__init__(**config)

        self._dialect = "clickhouse"

        # Determine deployment mode with priority:
        # 1. deployment_mode (from factory via colon syntax: clickhouse:local)
        # 2. mode (legacy config key)
        # Default to 'local' (easiest onboarding - no credentials required)
        deployment_mode = config.get("deployment_mode")
        legacy_mode = config.get("mode")

        if deployment_mode:
            self.deployment_mode = deployment_mode.lower()
        elif legacy_mode:
            self.deployment_mode = legacy_mode.lower()
            # Log deprecation warning for legacy mode config key
            logger.warning(
                "Config key 'mode' is deprecated. Use deployment mode syntax "
                "(clickhouse:local, clickhouse:server) or 'deployment_mode' config key."
            )
        else:
            self.deployment_mode = "local"  # Default to local (chDB)

        # Support 'embedded' as alias for 'local' for backward compatibility
        if self.deployment_mode == "embedded":
            self.deployment_mode = "local"

        # Validate deployment mode
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
                    "  3. Or use a different platform (e.g., DuckDB)\n"
                    "\nFor more information about chDB, visit: https://github.com/chdb-io/chdb"
                )
            self._setup_local_mode(config)
        elif self.deployment_mode == "cloud":
            # Check for clickhouse-connect dependency
            import importlib.util

            if importlib.util.find_spec("clickhouse_connect") is None:
                raise ImportError(
                    "ClickHouse Cloud mode requires clickhouse-connect but it is not installed.\n"
                    "To resolve this issue:\n"
                    "  1. Install clickhouse-connect: uv add clickhouse-connect\n"
                    "  2. Or use local mode: --platform clickhouse:local\n"
                    "\nFor more information, visit: https://clickhouse.com/docs/en/integrations/python"
                )
            self._setup_cloud_mode(config)

        # Store deployment_mode as mode for backward compatibility with existing code
        self.mode = self.deployment_mode


__all__ = ["ClickHouseAdapter"]
