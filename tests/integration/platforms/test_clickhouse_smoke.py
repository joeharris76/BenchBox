"""ClickHouse integration smoke tests with stubbed clickhouse-driver."""

import pytest

from .common import ClickHouseStubState, install_clickhouse_stub


@pytest.mark.integration
@pytest.mark.platform_smoke
def test_clickhouse_smoke_server_mode(monkeypatch, tmp_path):
    """Test basic ClickHouse adapter workflow in server mode."""
    state: ClickHouseStubState = install_clickhouse_stub(monkeypatch)

    from benchbox.platforms.clickhouse import ClickHouseAdapter

    adapter = ClickHouseAdapter(
        mode="server",
        host=state.host,
        port=state.port,
        database="benchbox_smoke",
    )

    connection = adapter.create_connection()
    try:
        info = adapter.get_platform_info(connection)
        assert info["platform_type"] == "clickhouse"
        assert "ClickHouse" in info["platform_name"]
    finally:
        adapter.close_connection(connection)


@pytest.mark.integration
@pytest.mark.platform_smoke
def test_clickhouse_requires_valid_mode(monkeypatch):
    """Test that ClickHouse adapter validates mode parameter."""
    install_clickhouse_stub(monkeypatch)

    from benchbox.platforms.clickhouse import ClickHouseAdapter

    with pytest.raises(ValueError) as excinfo:
        ClickHouseAdapter(mode="invalid_mode")

    assert "Invalid ClickHouse deployment mode" in str(excinfo.value)
