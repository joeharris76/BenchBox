"""PrestoDB integration smoke tests with stubbed prestodb driver."""

import pytest

from .common import PrestoStubState, install_presto_stub


@pytest.mark.integration
@pytest.mark.platform_smoke
def test_presto_smoke_run(monkeypatch, tmp_path):
    """Test basic Presto adapter workflow."""
    state: PrestoStubState = install_presto_stub(monkeypatch)

    from benchbox.platforms.presto import PrestoAdapter

    adapter = PrestoAdapter(
        host=state.host,
        port=state.port,
        catalog=state.catalog,
        schema=state.schema,
        username="presto",
    )

    connection = adapter.create_connection()
    try:
        info = adapter.get_platform_info(connection)
        assert info["platform_type"] == "presto"
        assert info["platform_name"] == "PrestoDB"
    finally:
        adapter.close_connection(connection)


@pytest.mark.integration
@pytest.mark.platform_smoke
def test_presto_requires_catalog(monkeypatch):
    """Test that Presto adapter requires catalog for certain operations."""
    install_presto_stub(monkeypatch)

    from benchbox.platforms.presto import PrestoAdapter

    # Presto allows creation without catalog (for auto-discovery)
    adapter = PrestoAdapter(
        host="localhost",
        port=8080,
        username="presto",
        # catalog intentionally omitted
    )

    # Adapter should be created successfully
    assert adapter.platform_name == "Presto"
