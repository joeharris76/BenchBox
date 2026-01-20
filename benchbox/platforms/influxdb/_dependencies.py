"""Optional dependencies for InfluxDB platform support.

InfluxDB 3.x uses FlightSQL for queries, which requires the influxdb3-python client
or the flightsql-dbapi library for DB-API 2.0 compliance.

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

# Primary client: influxdb3-python (recommended by InfluxData)
# The pip package is "influxdb3-python" but the module is "influxdb_client_3"
try:
    from influxdb_client_3 import InfluxDBClient3

    INFLUXDB3_AVAILABLE = True
except ImportError:  # pragma: no cover - optional dependency
    InfluxDBClient3 = None
    INFLUXDB3_AVAILABLE = False

# Alternative: flightsql-dbapi for DB-API 2.0 compliance
try:
    from flightsql import FlightSQLClient

    FLIGHTSQL_AVAILABLE = True
except ImportError:  # pragma: no cover - optional dependency
    FlightSQLClient = None
    FLIGHTSQL_AVAILABLE = False

# PyArrow is required for Arrow data handling
try:
    import pyarrow

    PYARROW_AVAILABLE = True
except ImportError:  # pragma: no cover - optional dependency
    pyarrow = None
    PYARROW_AVAILABLE = False

# Check if any InfluxDB client is available
INFLUXDB_AVAILABLE = INFLUXDB3_AVAILABLE or FLIGHTSQL_AVAILABLE

__all__ = [
    "InfluxDBClient3",
    "FlightSQLClient",
    "pyarrow",
    "INFLUXDB3_AVAILABLE",
    "FLIGHTSQL_AVAILABLE",
    "PYARROW_AVAILABLE",
    "INFLUXDB_AVAILABLE",
]
