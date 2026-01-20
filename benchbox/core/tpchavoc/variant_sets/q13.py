"""Variant definitions for Query 13."""

from __future__ import annotations

from benchbox.core.tpchavoc.variant_base import StaticSQLVariant

VARIANTS = {
    1: StaticSQLVariant(
        1,
        "Scalar subqueries: Use scalar subqueries for order counting",
        """\

select
    c_count,
    count(*) as custdist
from (
    select
        c_custkey,
        (select count(o2.o_orderkey)
         from orders o2
         where o2.o_custkey = c.c_custkey
           and o2.o_comment not like '%special%requests%') as c_count
    from customer c
) as c_orders
group by c_count
order by custdist desc, c_count desc
""",
    ),
    2: StaticSQLVariant(
        2,
        "HAVING clause: Use HAVING for distribution filtering",
        """\

select
    c_count,
    count(*) as custdist
from (
    select
        c_custkey,
        count(o_orderkey) as c_count
    from
        customer left outer join orders on
            c_custkey = o_custkey
            and o_comment not like '%special%requests%'
    group by
        c_custkey
) as c_orders
group by
    c_count
having
    count(*) > 0
order by
    custdist desc,
    c_count desc
""",
    ),
    3: StaticSQLVariant(
        3,
        "Explicit JOINs: Convert LEFT JOIN to explicit syntax",
        """\

select
    c_count,
    count(*) as custdist
from (
    select
        c.c_custkey,
        count(o.o_orderkey) as c_count
    from
        customer c
        left outer join orders o on c.c_custkey = o.o_custkey
                                 and o.o_comment not like '%special%requests%'
    group by
        c.c_custkey
) as c_orders (c_custkey, c_count)
group by
    c_count
order by
    custdist desc,
    c_count desc
""",
    ),
    4: StaticSQLVariant(
        4,
        "UNION ALL: Split customer analysis by segments",
        """\

select
    c_count,
    sum(custdist) as custdist
from (
    select c_count, count(*) as custdist
    from (
        select c_custkey, count(o_orderkey) as c_count
        from customer left outer join orders on c_custkey = o_custkey
             and o_comment not like '%special%requests%'
        where c_custkey % 2 = 0
        group by c_custkey
    ) c_orders_even
    group by c_count

    union all

    select c_count, count(*) as custdist
    from (
        select c_custkey, count(o_orderkey) as c_count
        from customer left outer join orders on c_custkey = o_custkey
             and o_comment not like '%special%requests%'
        where c_custkey % 2 = 1
        group by c_custkey
    ) c_orders_odd
    group by c_count
) combined
group by c_count
order by custdist desc, c_count desc
""",
    ),
    5: StaticSQLVariant(
        5,
        "Common Table Expression: Use CTEs to break down double aggregation",
        """\

with filtered_orders as (
    select o_orderkey, o_custkey
    from orders
    where o_comment not like '%special%requests%'
),
customer_order_counts as (
    select
        c.c_custkey,
        count(fo.o_orderkey) as c_count
    from
        customer c
        left outer join filtered_orders fo on c.c_custkey = fo.o_custkey
    group by
        c.c_custkey
),
distribution_analysis as (
    select c_count, count(*) as custdist
    from customer_order_counts
    group by c_count
)
select c_count, custdist
from distribution_analysis
order by custdist desc, c_count desc
""",
    ),
    6: StaticSQLVariant(
        6,
        "Derived table: Multiple levels of derived tables",
        """\

select
    order_count,
    customer_distribution
from (
    select
        c_count as order_count,
        count(*) as customer_distribution
    from (
        select
            customer_orders.c_custkey,
            customer_orders.c_count
        from (
            select
                c_custkey,
                count(o_orderkey) as c_count
            from
                customer left outer join orders on
                    c_custkey = o_custkey
                    and o_comment not like '%special%requests%'
            group by
                c_custkey
        ) as customer_orders
    ) as customer_summary
    group by
        c_count
) as distribution_summary
order by
    customer_distribution desc,
    order_count desc
""",
    ),
    7: StaticSQLVariant(
        7,
        "FILTER clause: Use FILTER for conditional counting (OLAP)",
        """\

select
    c_count,
    count(*) as custdist
from (
    select
        c_custkey,
        count(o_orderkey) filter (where o_comment not like '%special%requests%') as c_count
    from
        customer left outer join orders on c_custkey = o_custkey
    group by
        c_custkey
) as c_orders
group by
    c_count
order by
    custdist desc,
    c_count desc
""",
    ),
    8: StaticSQLVariant(
        8,
        "EXISTS pattern: Use EXISTS for order filtering",
        """\

select
    c_count,
    count(*) as custdist
from (
    select
        c_custkey,
        (select count(*)
         from orders o
         where o.o_custkey = c.c_custkey
           and not exists (
               select 1
               where o.o_comment like '%special%requests%'
           )) as c_count
    from customer c
) as c_orders
group by
    c_count
order by
    custdist desc,
    c_count desc
""",
    ),
    9: StaticSQLVariant(
        9,
        "Window functions: Use window functions for distribution analysis (OLAP)",
        """\

select distinct
    c_count,
    count(*) over (partition by c_count) as custdist,
    rank() over (order by count(*) over (partition by c_count) desc, c_count desc) as dist_rank
from (
    select
        c_custkey,
        count(o_orderkey) as c_count
    from
        customer left outer join orders on
            c_custkey = o_custkey
            and o_comment not like '%special%requests%'
    group by
        c_custkey
) as c_orders
order by
    custdist desc,
    c_count desc
""",
    ),
    10: StaticSQLVariant(
        10,
        "CASE expressions: Add extensive CASE logic for order classification",
        """\

select
    case
        when c_count = 0 then 0
        when c_count between 1 and 5 then c_count
        when c_count between 6 and 10 then c_count
        when c_count > 10 then c_count
        else c_count
    end as c_count,
    count(*) as custdist
from (
    select
        c_custkey,
        sum(
            case
                when case
                    when o_comment not like '%special%requests%' then 1
                    when o_comment is null then 1
                    else 0
                end = 1 then
                    case
                        when o_orderkey is not null then 1
                        else 0
                    end
                else 0
            end
        ) as c_count
    from
        customer left outer join orders on c_custkey = o_custkey
    group by
        c_custkey
) as c_orders
group by
    c_count
order by
    custdist desc,
    c_count desc
""",
    ),
}

__all__ = ["VARIANTS"]
