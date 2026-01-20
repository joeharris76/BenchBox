"""Optional dependencies for ClickHouse platform support."""

try:
    from clickhouse_driver import Client as ClickHouseClient
    from clickhouse_driver.errors import Error as ClickHouseError
except ImportError:  # pragma: no cover - optional dependency
    ClickHouseClient = None
    ClickHouseError = Exception

try:
    import clickhouse_connect
except ImportError:  # pragma: no cover - optional dependency
    clickhouse_connect = None

try:
    import chdb
except ImportError:  # pragma: no cover - optional dependency
    chdb = None

__all__ = ["ClickHouseClient", "ClickHouseError", "clickhouse_connect", "chdb"]
