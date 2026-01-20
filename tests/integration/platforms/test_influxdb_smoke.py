"""InfluxDB integration smoke tests with stubbed influxdb3-python client."""

import pytest

from .common import InfluxDBStubState, install_influxdb_stub


@pytest.mark.integration
@pytest.mark.platform_smoke
def test_influxdb_smoke_core_mode(monkeypatch, tmp_path):
    """Test basic InfluxDB adapter workflow in Core mode."""
    state: InfluxDBStubState = install_influxdb_stub(monkeypatch, mode="core")

    from benchbox.platforms.influxdb import InfluxDBAdapter

    adapter = InfluxDBAdapter(
        mode="core",
        host=state.host,
        port=state.port,
        token=state.token,
        database=state.database,
        ssl=False,
    )

    connection = adapter.create_connection()
    try:
        info = adapter.get_platform_info(connection)
        assert info["platform_type"] == "influxdb"
        assert "InfluxDB" in info["platform_name"]
        # Core mode should be in the platform name
        assert "Core" in info["platform_name"]
    finally:
        adapter.close_connection(connection)


@pytest.mark.integration
@pytest.mark.platform_smoke
def test_influxdb_smoke_cloud_mode(monkeypatch, tmp_path):
    """Test basic InfluxDB adapter workflow in Cloud mode."""
    state: InfluxDBStubState = install_influxdb_stub(
        monkeypatch,
        mode="cloud",
        host="us-east-1.aws.cloud2.influxdata.com",
    )

    from benchbox.platforms.influxdb import InfluxDBAdapter

    adapter = InfluxDBAdapter(
        mode="cloud",
        host=state.host,
        token=state.token,
        org="test-org",
        database=state.database,
    )

    connection = adapter.create_connection()
    try:
        info = adapter.get_platform_info(connection)
        assert info["platform_type"] == "influxdb"
        assert "Cloud" in info["platform_name"]
    finally:
        adapter.close_connection(connection)


@pytest.mark.integration
@pytest.mark.platform_smoke
def test_influxdb_requires_valid_mode(monkeypatch):
    """Test that InfluxDB adapter validates mode parameter."""
    install_influxdb_stub(monkeypatch)

    from benchbox.platforms.influxdb import InfluxDBAdapter

    with pytest.raises(ValueError) as excinfo:
        InfluxDBAdapter(
            mode="invalid_mode",
            host="localhost",
            token="test-token",
            database="test",
        )

    assert "Invalid InfluxDB mode" in str(excinfo.value)


@pytest.mark.integration
@pytest.mark.platform_smoke
def test_influxdb_query_execution(monkeypatch, tmp_path):
    """Test InfluxDB query execution via FlightSQL."""
    state: InfluxDBStubState = install_influxdb_stub(monkeypatch)

    from benchbox.platforms.influxdb import InfluxDBAdapter

    adapter = InfluxDBAdapter(
        mode="core",
        host=state.host,
        port=state.port,
        token=state.token,
        database=state.database,
        ssl=False,
    )

    connection = adapter.create_connection()
    try:
        # Execute a simple query
        result = connection.execute("SELECT 1")
        assert len(result) > 0

        # Verify query was recorded
        assert "SELECT 1" in state.statements
    finally:
        adapter.close_connection(connection)


@pytest.mark.integration
@pytest.mark.platform_smoke
def test_influxdb_table_row_count(monkeypatch, tmp_path):
    """Test getting table row counts from InfluxDB."""
    state: InfluxDBStubState = install_influxdb_stub(monkeypatch)
    state.row_counts["cpu"] = 1000

    from benchbox.platforms.influxdb import InfluxDBAdapter

    adapter = InfluxDBAdapter(
        mode="core",
        host=state.host,
        port=state.port,
        token=state.token,
        database=state.database,
        ssl=False,
    )

    connection = adapter.create_connection()
    try:
        count = adapter.get_table_row_count(connection, "cpu")
        assert count == 1000
    finally:
        adapter.close_connection(connection)


@pytest.mark.integration
@pytest.mark.platform_smoke
def test_influxdb_schema_auto_creation(monkeypatch, tmp_path):
    """Test that InfluxDB schema creation is a no-op (auto-created on write)."""
    state: InfluxDBStubState = install_influxdb_stub(monkeypatch)

    from benchbox.platforms.influxdb import InfluxDBAdapter

    adapter = InfluxDBAdapter(
        mode="core",
        host=state.host,
        port=state.port,
        token=state.token,
        database=state.database,
        ssl=False,
    )

    # Create a minimal benchmark stub
    class MockBenchmark:
        name = "test"

    connection = adapter.create_connection()
    try:
        # Schema creation should return 0.0 (no time spent)
        duration = adapter.create_schema(MockBenchmark(), connection)
        assert duration == 0.0

        # No SQL should be executed for schema creation
        schema_stmts = [s for s in state.statements if "CREATE" in s.upper()]
        assert len(schema_stmts) == 0
    finally:
        adapter.close_connection(connection)


@pytest.mark.integration
@pytest.mark.platform_smoke
def test_influxdb_get_tables(monkeypatch, tmp_path):
    """Test listing tables from InfluxDB."""
    state: InfluxDBStubState = install_influxdb_stub(monkeypatch)

    from benchbox.platforms.influxdb import InfluxDBAdapter

    adapter = InfluxDBAdapter(
        mode="core",
        host=state.host,
        port=state.port,
        token=state.token,
        database=state.database,
        ssl=False,
    )

    connection = adapter.create_connection()
    try:
        tables = adapter.get_tables(connection)
        # Stub returns ["cpu", "mem", "disk", "net"]
        assert "cpu" in tables
        assert "mem" in tables
    finally:
        adapter.close_connection(connection)
