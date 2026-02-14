from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest

from benchbox.platforms.clickhouse.metadata import ClickHouseMetadataMixin

pytestmark = pytest.mark.fast


class DummyClickHouse(ClickHouseMetadataMixin):
    def __init__(self, mode="local"):
        self.mode = mode
        self.data_path = "/tmp/ch"
        self.host = "localhost"
        self.port = 9000
        self.database = "bench"
        self.disable_result_cache = False
        self.logger = SimpleNamespace(debug=lambda *a, **k: None)


class TestClickHouseMetadataCoverage:
    def test_platform_name_and_target_dialect(self):
        assert DummyClickHouse(mode="local").platform_name == "ClickHouse (Local)"
        assert DummyClickHouse(mode="server").get_target_dialect() == "clickhouse"

    def test_get_database_path_variants(self, tmp_path):
        adapter = DummyClickHouse(mode="local")

        assert adapter.get_database_path(database_path="/tmp/db.duckdb").endswith(".chdb")
        assert adapter.get_database_path(database_path="/tmp/db").endswith(".chdb")

        fallback = adapter.get_database_path(database_name="bench_local")
        assert fallback.endswith("bench_local.chdb")

        server_adapter = DummyClickHouse(mode="server")
        assert server_adapter.get_database_path(database_path="/tmp/db.duckdb") is None

    def test_get_platform_info_local_no_connection(self):
        adapter = DummyClickHouse(mode="local")
        info = adapter.get_platform_info(connection=None)
        assert info["platform_type"] == "clickhouse"
        assert info["platform_version"] is None
        assert info["configuration"]["mode"] == "local"

    def test_get_platform_info_local_connection(self):
        adapter = DummyClickHouse(mode="local")

        class LocalConn:
            def query(self, _sql):
                return "24.10.1.1234\n"

        info = adapter.get_platform_info(connection=LocalConn())
        assert info["platform_version"] == "24.10.1.1234"

    def test_get_platform_info_server_connection(self):
        adapter = DummyClickHouse(mode="server")

        class Cursor:
            def __init__(self):
                self.calls = 0

            def execute(self, _sql):
                self.calls += 1

            def fetchone(self):
                return ("24.9",)

            def fetchall(self):
                if self.calls == 2:
                    return [("max_threads", "8")]
                if self.calls == 3:
                    return [("BUILD_TYPE", "Release")]
                return []

            def close(self):
                return None

        class Conn:
            def cursor(self):
                return Cursor()

        info = adapter.get_platform_info(connection=Conn())
        assert info["platform_version"] == "24.9"
        assert info["compute_configuration"]["system_settings"]["max_threads"] == "8"
        assert info["compute_configuration"]["build_options"]["BUILD_TYPE"] == "Release"
