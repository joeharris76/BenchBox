from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

pytestmark = pytest.mark.fast


class TestMotherDuckCoverage:
    def test_from_config_maps_cli_args(self):
        from benchbox.platforms.motherduck import MotherDuckAdapter

        with patch.dict("os.environ", {"MOTHERDUCK_TOKEN": "env-token"}, clear=True):
            adapter = MotherDuckAdapter.from_config(
                {
                    "benchmark": "tpch",
                    "motherduck_database": "bench_db",
                    "memory_limit": "8GB",
                }
            )

        assert adapter.database == "bench_db"
        assert adapter.memory_limit == "8GB"

    def test_create_connection_success_sets_memory_limit(self):
        from benchbox.platforms.motherduck import MotherDuckAdapter

        conn = MagicMock()
        conn.execute.side_effect = [MagicMock(), MagicMock(fetchone=lambda: (1,))]

        with patch("benchbox.platforms.motherduck.duckdb.connect", return_value=conn):
            adapter = MotherDuckAdapter(token="token", database="db1", memory_limit="2GB")
            created = adapter.create_connection()

        assert created is conn
        conn.execute.assert_any_call("SET memory_limit = '2GB'")

    def test_create_connection_failure_raises_connection_error(self):
        from benchbox.platforms.motherduck import MotherDuckAdapter

        with patch("benchbox.platforms.motherduck.duckdb.connect", side_effect=RuntimeError("no network")):
            adapter = MotherDuckAdapter(token="token", database="db1")
            with pytest.raises(ConnectionError, match="Failed to connect"):
                adapter.create_connection()

    def test_execute_query_success_and_error_paths(self):
        from benchbox.platforms.motherduck import MotherDuckAdapter

        adapter = MotherDuckAdapter(token="token")
        conn = MagicMock()
        conn.execute.return_value.fetchall.return_value = [(1,)]

        rows, elapsed = adapter.execute_query("SELECT 1", connection=conn)
        assert rows == [(1,)]
        assert elapsed >= 0

        conn.execute.side_effect = RuntimeError("bad query")
        with pytest.raises(RuntimeError, match="bad query"):
            adapter.execute_query("SELECT * FROM missing", connection=conn)

    def test_close_connection_and_metadata_with_connected_version(self):
        from benchbox.platforms.motherduck import MotherDuckAdapter

        adapter = MotherDuckAdapter(token="token", database="bench")
        conn = MagicMock()
        conn.execute.return_value.fetchone.return_value = ("v1",)
        adapter.connection = conn

        metadata = adapter.get_platform_metadata()
        assert metadata["duckdb_version"] == "v1"

        adapter.close_connection()
        assert adapter.connection is None

    def test_test_connection_false_on_exception(self):
        from benchbox.platforms.motherduck import MotherDuckAdapter

        adapter = MotherDuckAdapter(token="token")
        with patch.object(adapter, "create_connection", side_effect=RuntimeError("boom")):
            assert adapter.test_connection() is False
