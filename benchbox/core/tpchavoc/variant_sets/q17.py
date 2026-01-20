"""Variant definitions for Query 17."""

from __future__ import annotations

from benchbox.core.tpchavoc.variant_base import StaticSQLVariant

VARIANTS = {
    1: StaticSQLVariant(
        1,
        "Scalar subqueries: Use scalar subqueries for avg calculation",
        """\
select sum(l_extendedprice) / 7.0 as avg_yearly from lineitem, part where p_partkey = l_partkey and p_brand = 'Brand#23' and p_container = 'MED BOX' and l_quantity < (select 0.2 * avg(l_quantity) from lineitem where l_partkey = p_partkey)
""",
    ),
    2: StaticSQLVariant(
        2,
        "HAVING clause: Use HAVING for post-aggregation filtering",
        """\
with avg_qty as (select l_partkey, 0.2 * avg(l_quantity) as threshold from lineitem group by l_partkey) select sum(l_extendedprice) / 7.0 as avg_yearly from lineitem, part, avg_qty where p_partkey = l_partkey and p_brand = 'Brand#23' and p_container = 'MED BOX' and avg_qty.l_partkey = l_partkey and l_quantity < avg_qty.threshold
""",
    ),
    3: StaticSQLVariant(
        3,
        "Explicit JOINs: Replace comma-separated tables with explicit JOINs",
        """\
select sum(l_extendedprice) / 7.0 as avg_yearly from lineitem l inner join part p on p.p_partkey = l.l_partkey inner join (select l_partkey, 0.2 * avg(l_quantity) as threshold from lineitem group by l_partkey) avg_calc on l.l_partkey = avg_calc.l_partkey where p.p_brand = 'Brand#23' and p.p_container = 'MED BOX' and l.l_quantity < avg_calc.threshold
""",
    ),
    4: StaticSQLVariant(
        4,
        "UNION ALL: Split query by conditions",
        """\
select sum(revenue) / 7.0 as avg_yearly from (select l_extendedprice as revenue from lineitem, part where p_partkey = l_partkey and p_brand = 'Brand#23' and p_container = 'MED BOX' and l_quantity < (select 0.2 * avg(l_quantity) from lineitem where l_partkey = p_partkey) union all select 0 as revenue from dual where false) combined
""",
    ),
    5: StaticSQLVariant(
        5,
        "CTE: Use common table expressions for modular design",
        """\
with part_thresholds as (select l_partkey, 0.2 * avg(l_quantity) as avg_threshold from lineitem group by l_partkey), qualified_parts as (select p_partkey from part where p_brand = 'Brand#23' and p_container = 'MED BOX') select sum(l_extendedprice) / 7.0 as avg_yearly from lineitem l inner join qualified_parts qp on l.l_partkey = qp.p_partkey inner join part_thresholds pt on l.l_partkey = pt.l_partkey where l.l_quantity < pt.avg_threshold
""",
    ),
    6: StaticSQLVariant(
        6,
        "Derived table: Use derived tables for subqueries",
        """\
select sum(lineitem_data.l_extendedprice) / 7.0 as avg_yearly from (select l.l_extendedprice, l.l_quantity, l.l_partkey from lineitem l, part p, (select l_partkey as pk, 0.2 * avg(l_quantity) as threshold from lineitem group by l_partkey) avg_data where p.p_partkey = l.l_partkey and p.p_brand = 'Brand#23' and p.p_container = 'MED BOX' and avg_data.pk = l.l_partkey and l.l_quantity < avg_data.threshold) lineitem_data
""",
    ),
    7: StaticSQLVariant(
        7,
        "FILTER clause (OLAP): Use FILTER for conditional aggregation",
        """\
select sum(l_extendedprice) filter (where l_quantity < (select 0.2 * avg(l_quantity) from lineitem where l_partkey = p_partkey)) / 7.0 as avg_yearly from lineitem, part where p_partkey = l_partkey and p_brand = 'Brand#23' and p_container = 'MED BOX'
""",
    ),
    8: StaticSQLVariant(
        8,
        "EXISTS pattern: Use EXISTS for membership testing",
        """\
select sum(l_extendedprice) / 7.0 as avg_yearly from lineitem, part where p_partkey = l_partkey and p_brand = 'Brand#23' and p_container = 'MED BOX' and exists (select 1 from (select l_partkey, 0.2 * avg(l_quantity) as threshold from lineitem group by l_partkey) avg_calc where avg_calc.l_partkey = l_partkey and l_quantity < avg_calc.threshold)
""",
    ),
    9: StaticSQLVariant(
        9,
        "Window functions (OLAP): Use window functions for ranking",
        """\
with lineitem_with_avg as (select l_extendedprice, l_quantity, l_partkey, 0.2 * avg(l_quantity) over (partition by l_partkey) as avg_threshold from lineitem) select sum(lwa.l_extendedprice) / 7.0 as avg_yearly from lineitem_with_avg lwa, part p where p.p_partkey = lwa.l_partkey and p.p_brand = 'Brand#23' and p.p_container = 'MED BOX' and lwa.l_quantity < lwa.avg_threshold
""",
    ),
    10: StaticSQLVariant(
        10,
        "CASE expressions: Use CASE for conditional logic",
        """\
select sum(case when l_quantity < (select 0.2 * avg(l_quantity) from lineitem where l_partkey = p_partkey) then l_extendedprice else 0 end) / 7.0 as avg_yearly from lineitem, part where p_partkey = l_partkey and case when p_brand = 'Brand#23' then 1 else 0 end = 1 and case when p_container = 'MED BOX' then 1 else 0 end = 1
""",
    ),
}

__all__ = ["VARIANTS"]
