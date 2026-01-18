"""Variant definitions for Query 6."""

from __future__ import annotations

from benchbox.core.tpchavoc.variant_base import StaticSQLVariant

VARIANTS = {
    1: StaticSQLVariant(
        1,
        "Scalar subqueries: Use scalar subqueries for conditional aggregation",
        """\

select
    (select sum(l2.l_extendedprice * l2.l_discount)
     from lineitem l2
     where l2.l_shipdate >= date '1994-01-01'
       and l2.l_shipdate < date '1994-01-01' + interval '1' year
       and l2.l_discount between 0.06 - 0.01 and 0.06 + 0.01
       and l2.l_quantity < 24) as revenue
""",
    ),
    2: StaticSQLVariant(
        2,
        "HAVING clause: Use GROUP BY with HAVING for filtering",
        """\

select
    sum(l_extendedprice * l_discount) as revenue
from
    lineitem
where
    l_shipdate >= date '1994-01-01'
    and l_shipdate < date '1994-01-01' + interval '1' year
    and l_discount between 0.06 - 0.01 and 0.06 + 0.01
    and l_quantity < 24
group by
    ()
having
    sum(l_extendedprice * l_discount) is not null
""",
    ),
    3: StaticSQLVariant(
        3,
        "Multiple FROM clauses: Use redundant FROM for syntax variation",
        """\

select
    sum(l1.l_extendedprice * l1.l_discount) as revenue
from
    lineitem l1,
    (select 1 as dummy) dummy_table
where
    l1.l_shipdate >= date '1994-01-01'
    and l1.l_shipdate < date '1994-01-01' + interval '1' year
    and l1.l_discount between 0.06 - 0.01 and 0.06 + 0.01
    and l1.l_quantity < 24
""",
    ),
    4: StaticSQLVariant(
        4,
        "UNION ALL: Split aggregation by date ranges",
        """\

select
    sum(revenue) as revenue
from (
    select
        sum(l_extendedprice * l_discount) as revenue
    from lineitem
    where l_shipdate >= date '1994-01-01'
      and l_shipdate < date '1994-07-01'
      and l_discount between 0.06 - 0.01 and 0.06 + 0.01
      and l_quantity < 24

    union all

    select
        sum(l_extendedprice * l_discount) as revenue
    from lineitem
    where l_shipdate >= date '1994-07-01'
      and l_shipdate < date '1994-01-01' + interval '1' year
      and l_discount between 0.06 - 0.01 and 0.06 + 0.01
      and l_quantity < 24
) combined
""",
    ),
    5: StaticSQLVariant(
        5,
        "Common Table Expression: Use CTE to pre-filter data",
        """\

with qualified_lineitems as (
    select l_extendedprice, l_discount
    from lineitem
    where l_shipdate >= date '1994-01-01'
      and l_shipdate < date '1994-01-01' + interval '1' year
      and l_discount between 0.06 - 0.01 and 0.06 + 0.01
      and l_quantity < 24
)
select
    sum(l_extendedprice * l_discount) as revenue
from qualified_lineitems
""",
    ),
    6: StaticSQLVariant(
        6,
        "Derived table: Wrap main logic in derived table",
        """\

select
    total_revenue
from (
    select
        sum(l_extendedprice * l_discount) as total_revenue
    from
        lineitem
    where
        l_shipdate >= date '1994-01-01'
        and l_shipdate < date '1994-01-01' + interval '1' year
        and l_discount between 0.06 - 0.01 and 0.06 + 0.01
        and l_quantity < 24
) revenue_summary
""",
    ),
    7: StaticSQLVariant(
        7,
        "FILTER clause: Use FILTER for conditional aggregation (OLAP)",
        """\

select
    sum(l_extendedprice * l_discount)
        filter (where l_shipdate >= date '1994-01-01'
                   and l_shipdate < date '1994-01-01' + interval '1' year
                   and l_discount between 0.06 - 0.01 and 0.06 + 0.01
                   and l_quantity < 24) as revenue
from
    lineitem
""",
    ),
    8: StaticSQLVariant(
        8,
        "EXISTS pattern: Use EXISTS for conditional filtering",
        """\

select
    sum(l_extendedprice * l_discount) as revenue
from
    lineitem l1
where
    l1.l_shipdate >= date '1994-01-01'
    and l1.l_shipdate < date '1994-01-01' + interval '1' year
    and l1.l_discount between 0.06 - 0.01 and 0.06 + 0.01
    and l1.l_quantity < 24
    and exists (
        select 1
        from lineitem l2
        where l2.l_orderkey = l1.l_orderkey
          and l2.l_linenumber = l1.l_linenumber
    )
""",
    ),
    9: StaticSQLVariant(
        9,
        "Window functions: Use window functions for aggregation (OLAP)",
        """\

select distinct
    sum(l_extendedprice * l_discount) over () as revenue
from
    lineitem
where
    l_shipdate >= date '1994-01-01'
    and l_shipdate < date '1994-01-01' + interval '1' year
    and l_discount between 0.06 - 0.01 and 0.06 + 0.01
    and l_quantity < 24
""",
    ),
    10: StaticSQLVariant(
        10,
        "CASE expressions: Add extensive CASE logic for conditional processing",
        """\

select
    sum(
        case
            when l_shipdate >= date '1994-01-01'
                 and l_shipdate < date '1994-01-01' + interval '1' year
                 and l_discount between 0.06 - 0.01 and 0.06 + 0.01
                 and l_quantity < 24
            then case
                when l_discount between 0.05 and 0.07 then l_extendedprice * l_discount
                when l_discount < 0.05 then l_extendedprice * l_discount
                when l_discount > 0.07 then l_extendedprice * l_discount
                else l_extendedprice * l_discount
            end
            else 0
        end
    ) as revenue
from
    lineitem
where
    case
        when l_shipdate >= date '1994-01-01' then 1
        else 0
    end = 1
    and case
        when l_shipdate < date '1994-01-01' + interval '1' year then 1
        else 0
    end = 1
""",
    ),
}

__all__ = ["VARIANTS"]
