"""Variant definitions for Query 12."""

from __future__ import annotations

from benchbox.core.tpchavoc.variant_base import StaticSQLVariant

VARIANTS = {
    1: StaticSQLVariant(
        1,
        "Scalar subqueries: Use scalar subqueries for priority counting",
        """\

select
    l_shipmode,
    (select count(*)
     from orders o2, lineitem l2
     where o2.o_orderkey = l2.l_orderkey
       and l2.l_shipmode = l1.l_shipmode
       and (o2.o_orderpriority = '1-URGENT' or o2.o_orderpriority = '2-HIGH')
       and l2.l_commitdate < l2.l_receiptdate
       and l2.l_shipdate < l2.l_commitdate
       and l2.l_receiptdate >= date '1994-01-01'
       and l2.l_receiptdate < date '1994-01-01' + interval '1' year) as high_line_count,
    (select count(*)
     from orders o3, lineitem l3
     where o3.o_orderkey = l3.l_orderkey
       and l3.l_shipmode = l1.l_shipmode
       and (o3.o_orderpriority <> '1-URGENT' and o3.o_orderpriority <> '2-HIGH')
       and l3.l_commitdate < l3.l_receiptdate
       and l3.l_shipdate < l3.l_commitdate
       and l3.l_receiptdate >= date '1994-01-01'
       and l3.l_receiptdate < date '1994-01-01' + interval '1' year) as low_line_count
from (
    select distinct l_shipmode
    from orders, lineitem
    where o_orderkey = l_orderkey
      and l_shipmode in ('MAIL', 'SHIP')
      and l_commitdate < l_receiptdate
      and l_shipdate < l_commitdate
      and l_receiptdate >= date '1994-01-01'
      and l_receiptdate < date '1994-01-01' + interval '1' year
) l1
group by l_shipmode
order by l_shipmode
""",
    ),
    2: StaticSQLVariant(
        2,
        "HAVING clause: Use HAVING for shipmode filtering",
        """\

select
    l_shipmode,
    sum(case
        when o_orderpriority = '1-URGENT' or o_orderpriority = '2-HIGH'
        then 1 else 0
    end) as high_line_count,
    sum(case
        when o_orderpriority <> '1-URGENT' and o_orderpriority <> '2-HIGH'
        then 1 else 0
    end) as low_line_count
from
    orders,
    lineitem
where
    o_orderkey = l_orderkey
    and l_shipmode in ('MAIL', 'SHIP')
    and l_commitdate < l_receiptdate
    and l_shipdate < l_commitdate
    and l_receiptdate >= date '1994-01-01'
    and l_receiptdate < date '1994-01-01' + interval '1' year
group by
    l_shipmode
having
    count(*) > 0
    and sum(case when o_orderpriority = '1-URGENT' or o_orderpriority = '2-HIGH' then 1 else 0 end) >= 0
order by
    l_shipmode
""",
    ),
    3: StaticSQLVariant(
        3,
        "Explicit JOINs: Convert implicit joins to explicit JOIN syntax",
        """\

select
    l.l_shipmode,
    sum(case
        when o.o_orderpriority = '1-URGENT' or o.o_orderpriority = '2-HIGH'
        then 1 else 0
    end) as high_line_count,
    sum(case
        when o.o_orderpriority <> '1-URGENT' and o.o_orderpriority <> '2-HIGH'
        then 1 else 0
    end) as low_line_count
from
    orders o
    inner join lineitem l on o.o_orderkey = l.l_orderkey
where
    l.l_shipmode in ('MAIL', 'SHIP')
    and l.l_commitdate < l.l_receiptdate
    and l.l_shipdate < l.l_commitdate
    and l.l_receiptdate >= date '1994-01-01'
    and l.l_receiptdate < date '1994-01-01' + interval '1' year
group by
    l.l_shipmode
order by
    l.l_shipmode
""",
    ),
    4: StaticSQLVariant(
        4,
        "UNION ALL: Split by shipmode groups",
        """\

select
    l_shipmode,
    sum(high_line_count) as high_line_count,
    sum(low_line_count) as low_line_count
from (
    select
        l_shipmode,
        sum(case when o_orderpriority = '1-URGENT' or o_orderpriority = '2-HIGH' then 1 else 0 end) as high_line_count,
        sum(case when o_orderpriority <> '1-URGENT' and o_orderpriority <> '2-HIGH' then 1 else 0 end) as low_line_count
    from orders, lineitem
    where o_orderkey = l_orderkey
      and l_shipmode = 'MAIL'
      and l_commitdate < l_receiptdate
      and l_shipdate < l_commitdate
      and l_receiptdate >= date '1994-01-01'
      and l_receiptdate < date '1994-01-01' + interval '1' year
    group by l_shipmode

    union all

    select
        l_shipmode,
        sum(case when o_orderpriority = '1-URGENT' or o_orderpriority = '2-HIGH' then 1 else 0 end) as high_line_count,
        sum(case when o_orderpriority <> '1-URGENT' and o_orderpriority <> '2-HIGH' then 1 else 0 end) as low_line_count
    from orders, lineitem
    where o_orderkey = l_orderkey
      and l_shipmode = 'SHIP'
      and l_commitdate < l_receiptdate
      and l_shipdate < l_commitdate
      and l_receiptdate >= date '1994-01-01'
      and l_receiptdate < date '1994-01-01' + interval '1' year
    group by l_shipmode
) combined
group by l_shipmode
order by l_shipmode
""",
    ),
    5: StaticSQLVariant(
        5,
        "Common Table Expression: Use CTEs to break down priority analysis",
        """\

with qualified_shipments as (
    select l_shipmode, o_orderpriority
    from orders, lineitem
    where o_orderkey = l_orderkey
      and l_shipmode in ('MAIL', 'SHIP')
      and l_commitdate < l_receiptdate
      and l_shipdate < l_commitdate
      and l_receiptdate >= date '1994-01-01'
      and l_receiptdate < date '1994-01-01' + interval '1' year
),
priority_counts as (
    select
        l_shipmode,
        case when o_orderpriority = '1-URGENT' or o_orderpriority = '2-HIGH' then 1 else 0 end as is_high_priority,
        case when o_orderpriority <> '1-URGENT' and o_orderpriority <> '2-HIGH' then 1 else 0 end as is_low_priority
    from qualified_shipments
)
select
    l_shipmode,
    sum(is_high_priority) as high_line_count,
    sum(is_low_priority) as low_line_count
from priority_counts
group by l_shipmode
order by l_shipmode
""",
    ),
    6: StaticSQLVariant(
        6,
        "Derived table: Wrap main analysis in derived table",
        """\

select
    shipmode,
    high_priority_count,
    low_priority_count
from (
    select
        l_shipmode as shipmode,
        sum(case
            when o_orderpriority = '1-URGENT' or o_orderpriority = '2-HIGH'
            then 1 else 0
        end) as high_priority_count,
        sum(case
            when o_orderpriority <> '1-URGENT' and o_orderpriority <> '2-HIGH'
            then 1 else 0
        end) as low_priority_count
    from
        orders,
        lineitem
    where
        o_orderkey = l_orderkey
        and l_shipmode in ('MAIL', 'SHIP')
        and l_commitdate < l_receiptdate
        and l_shipdate < l_commitdate
        and l_receiptdate >= date '1994-01-01'
        and l_receiptdate < date '1994-01-01' + interval '1' year
    group by
        l_shipmode
) priority_analysis
order by
    shipmode
""",
    ),
    7: StaticSQLVariant(
        7,
        "FILTER clause: Use FILTER for conditional counting (OLAP)",
        """\

select
    l_shipmode,
    count(*) filter (where o_orderpriority = '1-URGENT' or o_orderpriority = '2-HIGH') as high_line_count,
    count(*) filter (where o_orderpriority <> '1-URGENT' and o_orderpriority <> '2-HIGH') as low_line_count
from
    orders,
    lineitem
where
    o_orderkey = l_orderkey
    and l_shipmode in ('MAIL', 'SHIP')
    and l_commitdate < l_receiptdate
    and l_shipdate < l_commitdate
    and l_receiptdate >= date '1994-01-01'
    and l_receiptdate < date '1994-01-01' + interval '1' year
group by
    l_shipmode
order by
    l_shipmode
""",
    ),
    8: StaticSQLVariant(
        8,
        "EXISTS pattern: Use EXISTS for shipmode filtering",
        """\

select
    l_shipmode,
    sum(case
        when o_orderpriority = '1-URGENT' or o_orderpriority = '2-HIGH'
        then 1 else 0
    end) as high_line_count,
    sum(case
        when o_orderpriority <> '1-URGENT' and o_orderpriority <> '2-HIGH'
        then 1 else 0
    end) as low_line_count
from
    orders,
    lineitem
where
    o_orderkey = l_orderkey
    and l_commitdate < l_receiptdate
    and l_shipdate < l_commitdate
    and l_receiptdate >= date '1994-01-01'
    and l_receiptdate < date '1994-01-01' + interval '1' year
    and exists (
        select 1
        where l_shipmode in ('MAIL', 'SHIP')
    )
group by
    l_shipmode
order by
    l_shipmode
""",
    ),
    9: StaticSQLVariant(
        9,
        "Window functions: Use window functions for shipmode analysis (OLAP)",
        """\

select distinct
    l_shipmode,
    sum(case when o_orderpriority = '1-URGENT' or o_orderpriority = '2-HIGH' then 1 else 0 end)
        over (partition by l_shipmode) as high_line_count,
    sum(case when o_orderpriority <> '1-URGENT' and o_orderpriority <> '2-HIGH' then 1 else 0 end)
        over (partition by l_shipmode) as low_line_count,
    rank() over (order by l_shipmode) as shipmode_rank
from
    orders,
    lineitem
where
    o_orderkey = l_orderkey
    and l_shipmode in ('MAIL', 'SHIP')
    and l_commitdate < l_receiptdate
    and l_shipdate < l_commitdate
    and l_receiptdate >= date '1994-01-01'
    and l_receiptdate < date '1994-01-01' + interval '1' year
order by
    l_shipmode
""",
    ),
    10: StaticSQLVariant(
        10,
        "CASE expressions: Add extensive CASE logic for priority classification",
        """\

select
    case
        when l_shipmode = 'MAIL' then 'MAIL'
        when l_shipmode = 'SHIP' then 'SHIP'
        when l_shipmode in ('AIR', 'AIR REG') then l_shipmode
        else 'OTHER'
    end as l_shipmode,
    sum(
        case
            when case
                when o_orderpriority = '1-URGENT' then 1
                when o_orderpriority = '2-HIGH' then 1
                else 0
            end = 1 then 1
            else 0
        end
    ) as high_line_count,
    sum(
        case
            when case
                when o_orderpriority <> '1-URGENT' and o_orderpriority <> '2-HIGH' then 1
                else 0
            end = 1 then 1
            else 0
        end
    ) as low_line_count
from
    orders,
    lineitem
where
    o_orderkey = l_orderkey
    and case
        when l_shipmode in ('MAIL', 'SHIP') then 1
        else 0
    end = 1
    and case
        when l_commitdate < l_receiptdate then 1
        else 0
    end = 1
    and case
        when l_shipdate < l_commitdate then 1
        else 0
    end = 1
    and case
        when l_receiptdate >= date '1994-01-01' then 1
        else 0
    end = 1
    and case
        when l_receiptdate < date '1994-01-01' + interval '1' year then 1
        else 0
    end = 1
group by
    l_shipmode
order by
    l_shipmode
""",
    ),
}

__all__ = ["VARIANTS"]
