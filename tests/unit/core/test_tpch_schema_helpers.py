import pytest

from benchbox.core.tpch import schema as tpch_schema

pytestmark = pytest.mark.fast


def test_column_get_sql_type_handles_sizes_and_defaults():
    Column = tpch_schema.Column
    DataType = tpch_schema.DataType
    assert Column("c1", DataType.VARCHAR, size=10).get_sql_type() == "VARCHAR(10)"
    assert Column("c2", DataType.CHAR, size=3).get_sql_type() == "CHAR(3)"
    assert Column("c3", DataType.DECIMAL).get_sql_type() == "DECIMAL(15,2)"
    assert Column("c4", DataType.INTEGER).get_sql_type() == "INTEGER"


def make_sample_table():
    Column = tpch_schema.Column
    DataType = tpch_schema.DataType
    cols = [
        Column("ID", DataType.INTEGER, primary_key=True),
        Column("NAME", DataType.VARCHAR, size=20, nullable=True),
        Column("REF", DataType.INTEGER, foreign_key=("T2", "ID")),
    ]
    return tpch_schema.Table("T1", cols)


def test_table_primary_and_foreign_key_helpers():
    t = make_sample_table()
    assert t.get_primary_key() == ["ID"]
    assert t.get_foreign_keys() == {"REF": ("T2", "ID")}


def test_create_table_sql_with_and_without_constraints():
    t = make_sample_table()
    sql = t.get_create_table_sql(enable_primary_keys=True, enable_foreign_keys=True)
    assert "CREATE TABLE T1" in sql
    assert "ID INTEGER NOT NULL" in sql
    assert "NAME VARCHAR(20)" in sql and "NOT NULL" not in sql.split("NAME VARCHAR(20)")[-1][:10]
    assert "FOREIGN KEY (REF) REFERENCES T2(ID)" in sql
    assert "PRIMARY KEY (ID)" in sql

    sql_no_pk = t.get_create_table_sql(enable_primary_keys=False, enable_foreign_keys=True)
    assert "PRIMARY KEY (ID)" not in sql_no_pk
    assert "FOREIGN KEY (REF) REFERENCES T2(ID)" in sql_no_pk

    sql_no_fk = t.get_create_table_sql(enable_primary_keys=True, enable_foreign_keys=False)
    assert "PRIMARY KEY (ID)" in sql_no_fk
    assert "FOREIGN KEY" not in sql_no_fk


def test_get_table_and_invalid():
    # Valid retrieval is case-insensitive via .lower in get_table
    t = tpch_schema.get_table("nation")
    assert t.name == "nation"
    with pytest.raises(ValueError):
        tpch_schema.get_table("not_a_table")


def test_get_create_all_tables_sql_toggles_constraints():
    sql_all = tpch_schema.get_create_all_tables_sql(True, True)
    assert "CREATE TABLE region" in sql_all and "CREATE TABLE nation" in sql_all
    assert "PRIMARY KEY" in sql_all and "FOREIGN KEY" in sql_all

    sql_no_constraints = tpch_schema.get_create_all_tables_sql(False, False)
    assert "PRIMARY KEY" not in sql_no_constraints
    assert "FOREIGN KEY" not in sql_no_constraints


def test_get_tunings_contains_expected_tables_and_columns():
    tunings = tpch_schema.get_tunings()
    # Has expected table tunings (lowercase per TPC spec)
    lt = tunings.get_table_tuning("lineitem")
    assert lt is not None and lt.partitioning and lt.clustering
    # Verify a sample column (lowercase)
    names = [c.name for c in lt.partitioning]
    assert "l_shipdate" in names
    # orders tuning exists with partitioning by o_orderdate
    ot = tunings.get_table_tuning("orders")
    assert ot is not None and ot.partitioning
    assert any(c.name == "o_orderdate" for c in ot.partitioning)
