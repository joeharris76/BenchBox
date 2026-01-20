"""Variant definitions for Query 3."""

from __future__ import annotations

from benchbox.core.tpchavoc.variant_base import StaticSQLVariant

VARIANTS = {
    1: StaticSQLVariant(
        1,
        "Scalar subqueries: Use scalar subqueries for revenue calculation",
        """\

select
    l_orderkey,
    (select sum(l2.l_extendedprice * (1 - l2.l_discount))
     from lineitem l2
     where l2.l_orderkey = l1.l_orderkey
       and l2.l_shipdate > date '1995-03-15') as revenue,
    o_orderdate,
    o_shippriority
from
    customer,
    orders,
    lineitem l1
where
    c_mktsegment = 'BUILDING'
    and c_custkey = o_custkey
    and l1.l_orderkey = o_orderkey
    and o_orderdate < date '1995-03-15'
    and l1.l_shipdate > date '1995-03-15'
group by
    l_orderkey,
    o_orderdate,
    o_shippriority
order by
    revenue desc,
    o_orderdate
""",
    ),
    2: StaticSQLVariant(
        2,
        "HAVING clause: Use HAVING for revenue filtering",
        """\

select
    l_orderkey,
    sum(l_extendedprice * (1 - l_discount)) as revenue,
    o_orderdate,
    o_shippriority
from
    customer,
    orders,
    lineitem
where
    c_mktsegment = 'BUILDING'
    and c_custkey = o_custkey
    and l_orderkey = o_orderkey
    and o_orderdate < date '1995-03-15'
    and l_shipdate > date '1995-03-15'
group by
    l_orderkey,
    o_orderdate,
    o_shippriority
having
    sum(l_extendedprice * (1 - l_discount)) is not null
order by
    revenue desc,
    o_orderdate
""",
    ),
    3: StaticSQLVariant(
        3,
        "Explicit JOINs: Convert implicit joins to explicit INNER JOIN syntax",
        """\

select
    l_orderkey,
    sum(l_extendedprice * (1 - l_discount)) as revenue,
    o_orderdate,
    o_shippriority
from
    customer c
    inner join orders o on c.c_custkey = o.o_custkey
    inner join lineitem l on o.o_orderkey = l.l_orderkey
where
    c.c_mktsegment = 'BUILDING'
    and o.o_orderdate < date '1995-03-15'
    and l.l_shipdate > date '1995-03-15'
group by
    l_orderkey,
    o_orderdate,
    o_shippriority
order by
    revenue desc,
    o_orderdate
""",
    ),
    4: StaticSQLVariant(
        4,
        "UNION ALL: Split by customer segments",
        """\

select
    l_orderkey,
    sum(l_extendedprice * (1 - l_discount)) as revenue,
    o_orderdate,
    o_shippriority
from (
    select l_orderkey, l_extendedprice, l_discount, o_orderdate, o_shippriority
    from customer, orders, lineitem
    where c_mktsegment = 'BUILDING'
      and c_custkey = o_custkey
      and l_orderkey = o_orderkey
      and o_orderdate < date '1995-03-15'
      and l_shipdate > date '1995-03-15'

    union all

    select l_orderkey, l_extendedprice, l_discount, o_orderdate, o_shippriority
    from customer, orders, lineitem
    where c_mktsegment = 'BUILDING'
      and c_custkey = o_custkey
      and l_orderkey = o_orderkey
      and o_orderdate < date '1995-03-15'
      and l_shipdate > date '1995-03-15'
      and 1 = 0  -- This branch will be empty, just for syntax variation
) combined
group by
    l_orderkey,
    o_orderdate,
    o_shippriority
order by
    revenue desc,
    o_orderdate
""",
    ),
    5: StaticSQLVariant(
        5,
        "Common Table Expression: Use CTE to break down the join logic",
        """\

with qualified_customers as (
    select c_custkey
    from customer
    where c_mktsegment = 'BUILDING'
),
qualified_orders as (
    select o_orderkey, o_orderdate, o_shippriority, o_custkey
    from orders
    where o_orderdate < date '1995-03-15'
),
qualified_lineitems as (
    select l_orderkey, l_extendedprice, l_discount
    from lineitem
    where l_shipdate > date '1995-03-15'
)
select
    ql.l_orderkey,
    sum(ql.l_extendedprice * (1 - ql.l_discount)) as revenue,
    qo.o_orderdate,
    qo.o_shippriority
from
    qualified_customers qc
    join qualified_orders qo on qc.c_custkey = qo.o_custkey
    join qualified_lineitems ql on qo.o_orderkey = ql.l_orderkey
group by
    ql.l_orderkey,
    qo.o_orderdate,
    qo.o_shippriority
order by
    revenue desc,
    qo.o_orderdate
""",
    ),
    6: StaticSQLVariant(
        6,
        "Derived table: Wrap main query in derived table",
        """\

select
    orderkey,
    total_revenue,
    orderdate,
    shippriority
from (
    select
        l_orderkey as orderkey,
        sum(l_extendedprice * (1 - l_discount)) as total_revenue,
        o_orderdate as orderdate,
        o_shippriority as shippriority
    from
        customer,
        orders,
        lineitem
    where
        c_mktsegment = 'BUILDING'
        and c_custkey = o_custkey
        and l_orderkey = o_orderkey
        and o_orderdate < date '1995-03-15'
        and l_shipdate > date '1995-03-15'
    group by
        l_orderkey,
        o_orderdate,
        o_shippriority
) revenue_summary
order by
    total_revenue desc,
    orderdate
""",
    ),
    7: StaticSQLVariant(
        7,
        "FILTER clause: Use FILTER for conditional aggregation (OLAP)",
        """\

select
    l_orderkey,
    sum(l_extendedprice * (1 - l_discount))
        filter (where l_shipdate > date '1995-03-15') as revenue,
    o_orderdate,
    o_shippriority
from
    customer,
    orders,
    lineitem
where
    c_mktsegment = 'BUILDING'
    and c_custkey = o_custkey
    and l_orderkey = o_orderkey
    and o_orderdate < date '1995-03-15'
group by
    l_orderkey,
    o_orderdate,
    o_shippriority
having
    revenue is not null
order by
    revenue desc,
    o_orderdate
""",
    ),
    8: StaticSQLVariant(
        8,
        "EXISTS pattern: Use EXISTS for join conditions",
        """\

select
    l_orderkey,
    sum(l_extendedprice * (1 - l_discount)) as revenue,
    o_orderdate,
    o_shippriority
from
    orders,
    lineitem
where
    l_orderkey = o_orderkey
    and o_orderdate < date '1995-03-15'
    and l_shipdate > date '1995-03-15'
    and exists (
        select 1
        from customer
        where c_custkey = o_custkey
          and c_mktsegment = 'BUILDING'
    )
group by
    l_orderkey,
    o_orderdate,
    o_shippriority
order by
    revenue desc,
    o_orderdate
""",
    ),
    9: StaticSQLVariant(
        9,
        "Window functions: Use window functions for revenue calculation (OLAP)",
        """\

select distinct
    l_orderkey,
    sum(l_extendedprice * (1 - l_discount)) over (partition by l_orderkey) as revenue,
    first_value(o_orderdate) over (partition by l_orderkey order by l_orderkey) as o_orderdate,
    first_value(o_shippriority) over (partition by l_orderkey order by l_orderkey) as o_shippriority
from
    customer,
    orders,
    lineitem
where
    c_mktsegment = 'BUILDING'
    and c_custkey = o_custkey
    and l_orderkey = o_orderkey
    and o_orderdate < date '1995-03-15'
    and l_shipdate > date '1995-03-15'
order by
    revenue desc,
    o_orderdate
""",
    ),
    10: StaticSQLVariant(
        10,
        "CASE expressions: Add extensive CASE logic for conditional processing",
        """\

select
    l_orderkey,
    sum(
        case
            when l_discount between 0.05 and 0.07 then l_extendedprice * (1 - l_discount) * 1.0
            when l_discount < 0.05 then l_extendedprice * (1 - l_discount) * 1.0
            when l_discount > 0.07 then l_extendedprice * (1 - l_discount) * 1.0
            else l_extendedprice * (1 - l_discount)
        end
    ) as revenue,
    o_orderdate,
    case
        when o_shippriority = 0 then 0
        else o_shippriority
    end as o_shippriority
from
    customer,
    orders,
    lineitem
where
    case
        when c_mktsegment = 'BUILDING' then c_custkey
        else null
    end = o_custkey
    and l_orderkey = o_orderkey
    and case
        when o_orderdate < date '1995-03-15' then 1
        else 0
    end = 1
    and case
        when l_shipdate > date '1995-03-15' then 1
        else 0
    end = 1
group by
    l_orderkey,
    o_orderdate,
    o_shippriority
order by
    revenue desc,
    o_orderdate
""",
    ),
}

__all__ = ["VARIANTS"]
