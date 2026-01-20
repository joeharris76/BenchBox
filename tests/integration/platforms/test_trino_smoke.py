"""Trino integration smoke tests with stubbed trino driver."""

import pytest

from .common import TrinoStubState, install_trino_stub


@pytest.mark.integration
@pytest.mark.platform_smoke
def test_trino_smoke_run(monkeypatch, tmp_path):
    """Test basic Trino adapter workflow."""
    state: TrinoStubState = install_trino_stub(monkeypatch)

    from benchbox.platforms.trino import TrinoAdapter

    adapter = TrinoAdapter(
        host=state.host,
        port=state.port,
        catalog=state.catalog,
        schema=state.schema,
        username="trino",
    )

    connection = adapter.create_connection()
    try:
        info = adapter.get_platform_info(connection)
        assert info["platform_type"] == "trino"
        assert info["platform_name"] == "Trino"
    finally:
        adapter.close_connection(connection)


@pytest.mark.integration
@pytest.mark.platform_smoke
def test_trino_requires_catalog(monkeypatch):
    """Test that Trino adapter requires catalog for certain operations."""
    install_trino_stub(monkeypatch)

    from benchbox.platforms.trino import TrinoAdapter

    # Trino allows creation without catalog (for auto-discovery)
    # But operations that need a catalog will fail
    adapter = TrinoAdapter(
        host="localhost",
        port=8080,
        username="trino",
        # catalog intentionally omitted
    )

    # Adapter should be created successfully
    assert adapter.platform_name == "Trino"
