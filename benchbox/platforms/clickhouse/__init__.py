"""ClickHouse platform package."""

from benchbox.utils.dependencies import check_platform_dependencies, get_dependency_error_message

from .adapter import ClickHouseAdapter
from .client import ClickHouseLocalClient
from .diagnostics import ClickHouseDiagnosticsMixin
from .metadata import ClickHouseMetadataMixin
from .setup import ClickHouseSetupMixin
from .tuning import ClickHouseTuningMixin
from .workload import ClickHouseWorkloadMixin

__all__ = [
    "ClickHouseAdapter",
    "ClickHouseLocalClient",
    "ClickHouseDiagnosticsMixin",
    "ClickHouseMetadataMixin",
    "ClickHouseSetupMixin",
    "ClickHouseTuningMixin",
    "ClickHouseWorkloadMixin",
    "check_platform_dependencies",
    "get_dependency_error_message",
]

# Register CLI platform options
try:
    from benchbox.cli.platform_hooks import PlatformHookRegistry, PlatformOptionSpec

    PlatformHookRegistry.register_option_specs(
        "clickhouse",
        PlatformOptionSpec(
            name="mode",
            choices=["server", "local"],
            default="server",
            help="ClickHouse connection mode: 'server' for remote/local server, 'local' for embedded chDB",
        ),
        PlatformOptionSpec(
            name="data_path",
            default=None,
            help="Path to chdb database file (local mode only)",
        ),
    )
except ImportError:
    # CLI module not available (e.g., when using core modules without CLI)
    pass
