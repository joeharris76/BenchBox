"""Variant definitions for Query 18."""

from __future__ import annotations

from benchbox.core.tpchavoc.variant_base import StaticSQLVariant

VARIANTS = {
    1: StaticSQLVariant(
        1,
        "Scalar subqueries: Use scalar subqueries for large orders",
        """\
select c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice, sum(l_quantity) from customer, orders, lineitem where o_orderkey in (select l_orderkey from lineitem group by l_orderkey having sum(l_quantity) > 300) and c_custkey = o_custkey and o_orderkey = l_orderkey group by c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice order by o_totalprice desc, o_orderdate limit 100
""",
    ),
    2: StaticSQLVariant(
        2,
        "HAVING clause: Use HAVING for post-aggregation filtering",
        """\
select c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice, sum(l_quantity) from customer, orders, lineitem where c_custkey = o_custkey and o_orderkey = l_orderkey group by c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice having sum(l_quantity) > 300 order by o_totalprice desc, o_orderdate limit 100
""",
    ),
    3: StaticSQLVariant(
        3,
        "Explicit JOINs: Replace comma-separated tables with explicit JOINs",
        """\
select c.c_name, c.c_custkey, o.o_orderkey, o.o_orderdate, o.o_totalprice, sum(l.l_quantity) from customer c inner join orders o on c.c_custkey = o.o_custkey inner join lineitem l on o.o_orderkey = l.l_orderkey inner join (select l_orderkey from lineitem group by l_orderkey having sum(l_quantity) > 300) large_orders on o.o_orderkey = large_orders.l_orderkey group by c.c_name, c.c_custkey, o.o_orderkey, o.o_orderdate, o.o_totalprice order by o.o_totalprice desc, o.o_orderdate limit 100
""",
    ),
    4: StaticSQLVariant(
        4,
        "UNION ALL: Split query by conditions",
        """\
select c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice, total_qty from (select c.c_name, c.c_custkey, o.o_orderkey, o.o_orderdate, o.o_totalprice, sum(l.l_quantity) as total_qty from customer c, orders o, lineitem l where c.c_custkey = o.o_custkey and o.o_orderkey = l.l_orderkey group by c.c_name, c.c_custkey, o.o_orderkey, o.o_orderdate, o.o_totalprice union all select 'DUMMY' as c_name, -1 as c_custkey, -1 as o_orderkey, date '1900-01-01' as o_orderdate, 0 as o_totalprice, 0 as total_qty where false) combined where total_qty > 300 order by o_totalprice desc, o_orderdate limit 100
""",
    ),
    5: StaticSQLVariant(
        5,
        "CTE: Use common table expressions for modular design",
        """\
with large_orders as (select l_orderkey from lineitem group by l_orderkey having sum(l_quantity) > 300) select c.c_name, c.c_custkey, o.o_orderkey, o.o_orderdate, o.o_totalprice, sum(l.l_quantity) from customer c, orders o, lineitem l, large_orders lo where c.c_custkey = o.o_custkey and o.o_orderkey = l.l_orderkey and o.o_orderkey = lo.l_orderkey group by c.c_name, c.c_custkey, o.o_orderkey, o.o_orderdate, o.o_totalprice order by o.o_totalprice desc, o.o_orderdate limit 100
""",
    ),
    6: StaticSQLVariant(
        6,
        "Derived table: Use derived tables for subqueries",
        """\
select order_data.c_name, order_data.c_custkey, order_data.o_orderkey, order_data.o_orderdate, order_data.o_totalprice, order_data.total_qty from (select c.c_name, c.c_custkey, o.o_orderkey, o.o_orderdate, o.o_totalprice, sum(l.l_quantity) as total_qty from customer c, orders o, lineitem l where c.c_custkey = o.o_custkey and o.o_orderkey = l.l_orderkey group by c.c_name, c.c_custkey, o.o_orderkey, o.o_orderdate, o.o_totalprice) order_data where order_data.total_qty > 300 order by order_data.o_totalprice desc, order_data.o_orderdate limit 100
""",
    ),
    7: StaticSQLVariant(
        7,
        "FILTER clause (OLAP): Use FILTER for conditional aggregation",
        """\
select c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice, sum(l_quantity) filter (where l_quantity > 0) from customer, orders, lineitem where o_orderkey in (select l_orderkey from lineitem group by l_orderkey having sum(l_quantity) > 300) and c_custkey = o_custkey and o_orderkey = l_orderkey group by c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice order by o_totalprice desc, o_orderdate limit 100
""",
    ),
    8: StaticSQLVariant(
        8,
        "EXISTS pattern: Use EXISTS for membership testing",
        """\
select c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice, sum(l_quantity) from customer, orders, lineitem where exists (select 1 from (select l_orderkey from lineitem group by l_orderkey having sum(l_quantity) > 300) large_orders where large_orders.l_orderkey = o_orderkey) and c_custkey = o_custkey and o_orderkey = l_orderkey group by c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice order by o_totalprice desc, o_orderdate limit 100
""",
    ),
    9: StaticSQLVariant(
        9,
        "Window functions (OLAP): Use window functions for ranking",
        """\
with order_quantities as (select c.c_name, c.c_custkey, o.o_orderkey, o.o_orderdate, o.o_totalprice, sum(l.l_quantity) as total_qty, rank() over (order by o.o_totalprice desc, o.o_orderdate) as price_rank from customer c, orders o, lineitem l where c.c_custkey = o.o_custkey and o.o_orderkey = l.l_orderkey group by c.c_name, c.c_custkey, o.o_orderkey, o.o_orderdate, o.o_totalprice having sum(l.l_quantity) > 300) select c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice, total_qty from order_quantities where price_rank <= 100
""",
    ),
    10: StaticSQLVariant(
        10,
        "CASE expressions: Use CASE for conditional logic",
        """\
select c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice, sum(case when l_quantity > 0 then l_quantity else 0 end) as total_qty from customer, orders, lineitem where case when o_orderkey in (select l_orderkey from lineitem group by l_orderkey having sum(l_quantity) > 300) then 1 else 0 end = 1 and c_custkey = o_custkey and o_orderkey = l_orderkey group by c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice order by o_totalprice desc, o_orderdate limit 100
""",
    ),
}

__all__ = ["VARIANTS"]
