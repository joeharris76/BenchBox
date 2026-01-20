from pathlib import Path

import pytest

from benchbox.platforms.sqlite import SQLiteAdapter

pytestmark = pytest.mark.fast


class _StubBenchmark:
    # No tables mapping and no get_tables to force manifest fallback
    pass


def test_sqlite_manifest_fallback_loads_data(tmp_path: Path):
    data_dir = tmp_path / "run"
    data_dir.mkdir()

    # Create simple data file
    f = data_dir / "test.tbl"
    f.write_text("1|a\n2|b\n")

    # Create manifest referencing the file
    manifest = {
        "benchmark": "tpch",
        "scale_factor": 0.01,
        "compression": {"enabled": False, "type": "none", "level": None},
        "parallel": 1,
        "created_at": "2025-01-01T00:00:00Z",
        "generator_version": "v1",
        "tables": {"test": [{"path": f.name, "size_bytes": f.stat().st_size, "row_count": 2}]},
    }
    import json

    (data_dir / "_datagen_manifest.json").write_text(json.dumps(manifest))

    # Set up SQLite adapter and connection
    adapter = SQLiteAdapter(database_path=":memory:")
    conn = adapter.create_connection(database_path=":memory:")

    # Create target table manually (two text columns) - lowercase per TPC spec
    conn.execute("CREATE TABLE test (c1 TEXT, c2 TEXT)")
    conn.commit()

    # Run load_data using manifest fallback
    stats, _, _ = adapter.load_data(_StubBenchmark(), conn, data_dir)

    assert stats.get("test") == 2
    # Verify data loaded
    rows = conn.execute("SELECT COUNT(*) FROM test").fetchone()[0]
    assert rows == 2
