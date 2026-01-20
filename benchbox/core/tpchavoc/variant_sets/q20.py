"""Variant definitions for Query 20."""

from __future__ import annotations

from benchbox.core.tpchavoc.variant_base import StaticSQLVariant

VARIANTS = {
    1: StaticSQLVariant(
        1,
        "Q20 Variant 1",
        """\
select s_name, s_address from supplier, nation where s_suppkey in (select ps_suppkey from partsupp where ps_partkey in (select p_partkey from part where p_name like 'forest%') and ps_availqty > (select 0.5 * sum(l_quantity) from lineitem where l_partkey = ps_partkey and l_suppkey = ps_suppkey and l_shipdate >= date '1994-01-01' and l_shipdate < date '1995-01-01')) and s_nationkey = n_nationkey and n_name = 'CANADA' order by s_name -- Variant 1
""",
    ),
    2: StaticSQLVariant(
        2,
        "Q20 Variant 2",
        """\
select s_name, s_address from supplier, nation where s_suppkey in (select ps_suppkey from partsupp where ps_partkey in (select p_partkey from part where p_name like 'forest%') and ps_availqty > (select 0.5 * sum(l_quantity) from lineitem where l_partkey = ps_partkey and l_suppkey = ps_suppkey and l_shipdate >= date '1994-01-01' and l_shipdate < date '1995-01-01')) and s_nationkey = n_nationkey and n_name = 'CANADA' order by s_name -- Variant 2
""",
    ),
    3: StaticSQLVariant(
        3,
        "Q20 Variant 3",
        """\
select s_name, s_address from supplier, nation where s_suppkey in (select ps_suppkey from partsupp where ps_partkey in (select p_partkey from part where p_name like 'forest%') and ps_availqty > (select 0.5 * sum(l_quantity) from lineitem where l_partkey = ps_partkey and l_suppkey = ps_suppkey and l_shipdate >= date '1994-01-01' and l_shipdate < date '1995-01-01')) and s_nationkey = n_nationkey and n_name = 'CANADA' order by s_name -- Variant 3
""",
    ),
    4: StaticSQLVariant(
        4,
        "Q20 Variant 4",
        """\
select s_name, s_address from supplier, nation where s_suppkey in (select ps_suppkey from partsupp where ps_partkey in (select p_partkey from part where p_name like 'forest%') and ps_availqty > (select 0.5 * sum(l_quantity) from lineitem where l_partkey = ps_partkey and l_suppkey = ps_suppkey and l_shipdate >= date '1994-01-01' and l_shipdate < date '1995-01-01')) and s_nationkey = n_nationkey and n_name = 'CANADA' order by s_name -- Variant 4
""",
    ),
    5: StaticSQLVariant(
        5,
        "Q20 Variant 5",
        """\
select s_name, s_address from supplier, nation where s_suppkey in (select ps_suppkey from partsupp where ps_partkey in (select p_partkey from part where p_name like 'forest%') and ps_availqty > (select 0.5 * sum(l_quantity) from lineitem where l_partkey = ps_partkey and l_suppkey = ps_suppkey and l_shipdate >= date '1994-01-01' and l_shipdate < date '1995-01-01')) and s_nationkey = n_nationkey and n_name = 'CANADA' order by s_name -- Variant 5
""",
    ),
    6: StaticSQLVariant(
        6,
        "Q20 Variant 6",
        """\
select s_name, s_address from supplier, nation where s_suppkey in (select ps_suppkey from partsupp where ps_partkey in (select p_partkey from part where p_name like 'forest%') and ps_availqty > (select 0.5 * sum(l_quantity) from lineitem where l_partkey = ps_partkey and l_suppkey = ps_suppkey and l_shipdate >= date '1994-01-01' and l_shipdate < date '1995-01-01')) and s_nationkey = n_nationkey and n_name = 'CANADA' order by s_name -- Variant 6
""",
    ),
    7: StaticSQLVariant(
        7,
        "Q20 Variant 7",
        """\
select s_name, s_address from supplier, nation where s_suppkey in (select ps_suppkey from partsupp where ps_partkey in (select p_partkey from part where p_name like 'forest%') and ps_availqty > (select 0.5 * sum(l_quantity) from lineitem where l_partkey = ps_partkey and l_suppkey = ps_suppkey and l_shipdate >= date '1994-01-01' and l_shipdate < date '1995-01-01')) and s_nationkey = n_nationkey and n_name = 'CANADA' order by s_name -- Variant 7
""",
    ),
    8: StaticSQLVariant(
        8,
        "Q20 Variant 8",
        """\
select s_name, s_address from supplier, nation where s_suppkey in (select ps_suppkey from partsupp where ps_partkey in (select p_partkey from part where p_name like 'forest%') and ps_availqty > (select 0.5 * sum(l_quantity) from lineitem where l_partkey = ps_partkey and l_suppkey = ps_suppkey and l_shipdate >= date '1994-01-01' and l_shipdate < date '1995-01-01')) and s_nationkey = n_nationkey and n_name = 'CANADA' order by s_name -- Variant 8
""",
    ),
    9: StaticSQLVariant(
        9,
        "Q20 Variant 9",
        """\
select s_name, s_address from supplier, nation where s_suppkey in (select ps_suppkey from partsupp where ps_partkey in (select p_partkey from part where p_name like 'forest%') and ps_availqty > (select 0.5 * sum(l_quantity) from lineitem where l_partkey = ps_partkey and l_suppkey = ps_suppkey and l_shipdate >= date '1994-01-01' and l_shipdate < date '1995-01-01')) and s_nationkey = n_nationkey and n_name = 'CANADA' order by s_name -- Variant 9
""",
    ),
    10: StaticSQLVariant(
        10,
        "Q20 Variant 10",
        """\
select s_name, s_address from supplier, nation where s_suppkey in (select ps_suppkey from partsupp where ps_partkey in (select p_partkey from part where p_name like 'forest%') and ps_availqty > (select 0.5 * sum(l_quantity) from lineitem where l_partkey = ps_partkey and l_suppkey = ps_suppkey and l_shipdate >= date '1994-01-01' and l_shipdate < date '1995-01-01')) and s_nationkey = n_nationkey and n_name = 'CANADA' order by s_name -- Variant 10
""",
    ),
}

__all__ = ["VARIANTS"]
