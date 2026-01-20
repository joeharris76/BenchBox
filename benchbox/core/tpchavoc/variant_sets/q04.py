"""Variant definitions for Query 4."""

from __future__ import annotations

from benchbox.core.tpchavoc.variant_base import StaticSQLVariant

VARIANTS = {
    1: StaticSQLVariant(
        1,
        "Scalar subqueries: Use scalar subquery for existence check",
        """\

select
    o_orderpriority,
    count(*) as order_count
from
    orders
where
    o_orderdate >= date '1993-07-01'
    and o_orderdate < date '1993-07-01' + interval '3' month
    and (
        select count(*)
        from lineitem
        where l_orderkey = o_orderkey
          and l_commitdate < l_receiptdate
    ) > 0
group by
    o_orderpriority
order by
    o_orderpriority
""",
    ),
    2: StaticSQLVariant(
        2,
        "HAVING clause: Use HAVING for existence filtering",
        """\

select
    o_orderpriority,
    count(*) as order_count
from
    orders
where
    o_orderdate >= date '1993-07-01'
    and o_orderdate < date '1993-07-01' + interval '3' month
    and exists (
        select *
        from lineitem
        where l_orderkey = o_orderkey
          and l_commitdate < l_receiptdate
    )
group by
    o_orderpriority
having
    count(*) > 0
order by
    o_orderpriority
""",
    ),
    3: StaticSQLVariant(
        3,
        "Explicit JOINs: Convert EXISTS to explicit semi-join",
        """\

select
    o_orderpriority,
    count(*) as order_count
from
    orders o
    inner join (
        select distinct l_orderkey
        from lineitem
        where l_commitdate < l_receiptdate
    ) qualified_lineitems on o.o_orderkey = qualified_lineitems.l_orderkey
where
    o.o_orderdate >= date '1993-07-01'
    and o.o_orderdate < date '1993-07-01' + interval '3' month
group by
    o_orderpriority
order by
    o_orderpriority
""",
    ),
    4: StaticSQLVariant(
        4,
        "UNION ALL: Split by order priority",
        """\

select
    priority_group,
    sum(order_count) as order_count
from (
    select
        o_orderpriority as priority_group,
        count(*) as order_count
    from orders
    where o_orderdate >= date '1993-07-01'
      and o_orderdate < date '1993-07-01' + interval '3' month
      and o_orderpriority in ('1-URGENT', '2-HIGH')
      and exists (
          select *
          from lineitem
          where l_orderkey = o_orderkey
            and l_commitdate < l_receiptdate
      )
    group by o_orderpriority

    union all

    select
        o_orderpriority as priority_group,
        count(*) as order_count
    from orders
    where o_orderdate >= date '1993-07-01'
      and o_orderdate < date '1993-07-01' + interval '3' month
      and o_orderpriority not in ('1-URGENT', '2-HIGH')
      and exists (
          select *
          from lineitem
          where l_orderkey = o_orderkey
            and l_commitdate < l_receiptdate
      )
    group by o_orderpriority
) combined
group by priority_group
order by priority_group
""",
    ),
    5: StaticSQLVariant(
        5,
        "Common Table Expression: Use CTE to pre-filter qualified orders",
        """\

with qualified_lineitems as (
    select distinct l_orderkey
    from lineitem
    where l_commitdate < l_receiptdate
),
qualified_orders as (
    select o_orderkey, o_orderpriority
    from orders
    where o_orderdate >= date '1993-07-01'
      and o_orderdate < date '1993-07-01' + interval '3' month
)
select
    qo.o_orderpriority,
    count(*) as order_count
from
    qualified_orders qo
    inner join qualified_lineitems ql on qo.o_orderkey = ql.l_orderkey
group by
    qo.o_orderpriority
order by
    qo.o_orderpriority
""",
    ),
    6: StaticSQLVariant(
        6,
        "Derived table: Wrap main query in derived table",
        """\

select
    orderpriority,
    total_count
from (
    select
        o_orderpriority as orderpriority,
        count(*) as total_count
    from
        orders
    where
        o_orderdate >= date '1993-07-01'
        and o_orderdate < date '1993-07-01' + interval '3' month
        and exists (
            select *
            from lineitem
            where l_orderkey = o_orderkey
              and l_commitdate < l_receiptdate
        )
    group by
        o_orderpriority
) priority_summary
order by
    orderpriority
""",
    ),
    7: StaticSQLVariant(
        7,
        "FILTER clause: Use FILTER for conditional counting (OLAP)",
        """\

select
    o_orderpriority,
    count(*) filter (where exists (
        select *
        from lineitem
        where l_orderkey = o_orderkey
          and l_commitdate < l_receiptdate
    )) as order_count
from
    orders
where
    o_orderdate >= date '1993-07-01'
    and o_orderdate < date '1993-07-01' + interval '3' month
group by
    o_orderpriority
having
    order_count > 0
order by
    o_orderpriority
""",
    ),
    8: StaticSQLVariant(
        8,
        "IN pattern: Convert EXISTS to IN with subquery",
        """\

select
    o_orderpriority,
    count(*) as order_count
from
    orders
where
    o_orderdate >= date '1993-07-01'
    and o_orderdate < date '1993-07-01' + interval '3' month
    and o_orderkey in (
        select l_orderkey
        from lineitem
        where l_commitdate < l_receiptdate
    )
group by
    o_orderpriority
order by
    o_orderpriority
""",
    ),
    9: StaticSQLVariant(
        9,
        "Window functions: Use window functions for priority analysis (OLAP)",
        """\

select distinct
    o_orderpriority,
    count(*) over (partition by o_orderpriority) as order_count
from
    orders
where
    o_orderdate >= date '1993-07-01'
    and o_orderdate < date '1993-07-01' + interval '3' month
    and exists (
        select *
        from lineitem
        where l_orderkey = o_orderkey
          and l_commitdate < l_receiptdate
    )
order by
    o_orderpriority
""",
    ),
    10: StaticSQLVariant(
        10,
        "CASE expressions: Add extensive CASE logic for priority classification",
        """\

select
    case
        when o_orderpriority = '1-URGENT' then '1-URGENT'
        when o_orderpriority = '2-HIGH' then '2-HIGH'
        when o_orderpriority = '3-MEDIUM' then '3-MEDIUM'
        when o_orderpriority = '4-NOT SPECIFIED' then '4-NOT SPECIFIED'
        when o_orderpriority = '5-LOW' then '5-LOW'
        else o_orderpriority
    end as o_orderpriority,
    count(
        case
            when exists (
                select *
                from lineitem
                where l_orderkey = o_orderkey
                  and l_commitdate < l_receiptdate
            ) then 1
            else null
        end
    ) as order_count
from
    orders
where
    case
        when o_orderdate >= date '1993-07-01' then 1
        else 0
    end = 1
    and case
        when o_orderdate < date '1993-07-01' + interval '3' month then 1
        else 0
    end = 1
group by
    o_orderpriority
order by
    o_orderpriority
""",
    ),
}

__all__ = ["VARIANTS"]
