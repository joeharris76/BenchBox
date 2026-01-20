"""Variant definitions for Query 10."""

from __future__ import annotations

from benchbox.core.tpchavoc.variant_base import StaticSQLVariant

VARIANTS = {
    1: StaticSQLVariant(
        1,
        "Scalar subqueries: Use scalar subqueries for revenue calculation",
        """\

select
    c_custkey,
    c_name,
    (select sum(l2.l_extendedprice * (1 - l2.l_discount))
     from orders o2, lineitem l2
     where c_custkey = o2.o_custkey
       and l2.l_orderkey = o2.o_orderkey
       and o2.o_orderdate >= date '1993-10-01'
       and o2.o_orderdate < date '1993-10-01' + interval '3' month
       and l2.l_returnflag = 'R') as revenue,
    c_acctbal,
    n_name,
    c_address,
    c_phone,
    c_comment
from
    customer,
    nation
where
    c_nationkey = n_nationkey
    and exists (
        select 1
        from orders, lineitem
        where c_custkey = o_custkey
          and l_orderkey = o_orderkey
          and o_orderdate >= date '1993-10-01'
          and o_orderdate < date '1993-10-01' + interval '3' month
          and l_returnflag = 'R'
    )
group by
    c_custkey,
    c_name,
    c_acctbal,
    c_phone,
    n_name,
    c_address,
    c_comment
order by
    revenue desc
""",
    ),
    2: StaticSQLVariant(
        2,
        "HAVING clause: Use HAVING for revenue filtering",
        """\

select
    c_custkey,
    c_name,
    sum(l_extendedprice * (1 - l_discount)) as revenue,
    c_acctbal,
    n_name,
    c_address,
    c_phone,
    c_comment
from
    customer,
    orders,
    lineitem,
    nation
where
    c_custkey = o_custkey
    and l_orderkey = o_orderkey
    and o_orderdate >= date '1993-10-01'
    and o_orderdate < date '1993-10-01' + interval '3' month
    and l_returnflag = 'R'
    and c_nationkey = n_nationkey
group by
    c_custkey,
    c_name,
    c_acctbal,
    c_phone,
    n_name,
    c_address,
    c_comment
having
    sum(l_extendedprice * (1 - l_discount)) > 0
order by
    revenue desc
""",
    ),
    3: StaticSQLVariant(
        3,
        "Explicit JOINs: Convert all implicit joins to explicit JOIN syntax",
        """\

select
    c.c_custkey,
    c.c_name,
    sum(l.l_extendedprice * (1 - l.l_discount)) as revenue,
    c.c_acctbal,
    n.n_name,
    c.c_address,
    c.c_phone,
    c.c_comment
from
    customer c
    inner join orders o on c.c_custkey = o.o_custkey
    inner join lineitem l on o.o_orderkey = l.l_orderkey
    inner join nation n on c.c_nationkey = n.n_nationkey
where
    o.o_orderdate >= date '1993-10-01'
    and o.o_orderdate < date '1993-10-01' + interval '3' month
    and l.l_returnflag = 'R'
group by
    c.c_custkey,
    c.c_name,
    c.c_acctbal,
    c.c_phone,
    n.n_name,
    c.c_address,
    c.c_comment
order by
    revenue desc
""",
    ),
    4: StaticSQLVariant(
        4,
        "UNION ALL: Split by customer segments",
        """\

select
    c_custkey,
    c_name,
    sum(revenue) as revenue,
    c_acctbal,
    n_name,
    c_address,
    c_phone,
    c_comment
from (
    select
        c_custkey,
        c_name,
        sum(l_extendedprice * (1 - l_discount)) as revenue,
        c_acctbal,
        n_name,
        c_address,
        c_phone,
        c_comment
    from customer, orders, lineitem, nation
    where c_custkey = o_custkey
      and l_orderkey = o_orderkey
      and o_orderdate >= date '1993-10-01'
      and o_orderdate < date '1993-10-01' + interval '3' month
      and l_returnflag = 'R'
      and c_nationkey = n_nationkey
      and c_acctbal > 0
    group by c_custkey, c_name, c_acctbal, c_phone, n_name, c_address, c_comment

    union all

    select
        c_custkey,
        c_name,
        sum(l_extendedprice * (1 - l_discount)) as revenue,
        c_acctbal,
        n_name,
        c_address,
        c_phone,
        c_comment
    from customer, orders, lineitem, nation
    where c_custkey = o_custkey
      and l_orderkey = o_orderkey
      and o_orderdate >= date '1993-10-01'
      and o_orderdate < date '1993-10-01' + interval '3' month
      and l_returnflag = 'R'
      and c_nationkey = n_nationkey
      and c_acctbal <= 0
    group by c_custkey, c_name, c_acctbal, c_phone, n_name, c_address, c_comment
) combined
group by
    c_custkey,
    c_name,
    c_acctbal,
    c_phone,
    n_name,
    c_address,
    c_comment
order by
    revenue desc
""",
    ),
    5: StaticSQLVariant(
        5,
        "Common Table Expression: Use CTEs to break down customer returns analysis",
        """\

with qualified_orders as (
    select o_orderkey, o_custkey
    from orders
    where o_orderdate >= date '1993-10-01'
      and o_orderdate < date '1993-10-01' + interval '3' month
),
returned_lineitems as (
    select l_orderkey, l_extendedprice, l_discount
    from lineitem
    where l_returnflag = 'R'
),
customer_nations as (
    select c_custkey, c_name, c_acctbal, c_address, c_phone, c_comment, n_name
    from customer, nation
    where c_nationkey = n_nationkey
),
customer_returns as (
    select
        cn.c_custkey,
        cn.c_name,
        sum(rl.l_extendedprice * (1 - rl.l_discount)) as revenue,
        cn.c_acctbal,
        cn.n_name,
        cn.c_address,
        cn.c_phone,
        cn.c_comment
    from
        customer_nations cn
        join qualified_orders qo on cn.c_custkey = qo.o_custkey
        join returned_lineitems rl on qo.o_orderkey = rl.l_orderkey
    group by
        cn.c_custkey,
        cn.c_name,
        cn.c_acctbal,
        cn.c_phone,
        cn.n_name,
        cn.c_address,
        cn.c_comment
)
select
    c_custkey,
    c_name,
    revenue,
    c_acctbal,
    n_name,
    c_address,
    c_phone,
    c_comment
from customer_returns
order by
    revenue desc
""",
    ),
    6: StaticSQLVariant(
        6,
        "Derived table: Wrap main analysis in derived table",
        """\

select
    custkey,
    custname,
    total_revenue,
    acctbal,
    nation,
    address,
    phone,
    comment
from (
    select
        c_custkey as custkey,
        c_name as custname,
        sum(l_extendedprice * (1 - l_discount)) as total_revenue,
        c_acctbal as acctbal,
        n_name as nation,
        c_address as address,
        c_phone as phone,
        c_comment as comment
    from
        customer,
        orders,
        lineitem,
        nation
    where
        c_custkey = o_custkey
        and l_orderkey = o_orderkey
        and o_orderdate >= date '1993-10-01'
        and o_orderdate < date '1993-10-01' + interval '3' month
        and l_returnflag = 'R'
        and c_nationkey = n_nationkey
    group by
        c_custkey,
        c_name,
        c_acctbal,
        c_phone,
        n_name,
        c_address,
        c_comment
) customer_revenue
order by
    total_revenue desc
""",
    ),
    7: StaticSQLVariant(
        7,
        "FILTER clause: Use FILTER for conditional revenue aggregation (OLAP)",
        """\

select
    c_custkey,
    c_name,
    sum(l_extendedprice * (1 - l_discount))
        filter (where l_returnflag = 'R') as revenue,
    c_acctbal,
    n_name,
    c_address,
    c_phone,
    c_comment
from
    customer,
    orders,
    lineitem,
    nation
where
    c_custkey = o_custkey
    and l_orderkey = o_orderkey
    and o_orderdate >= date '1993-10-01'
    and o_orderdate < date '1993-10-01' + interval '3' month
    and c_nationkey = n_nationkey
group by
    c_custkey,
    c_name,
    c_acctbal,
    c_phone,
    n_name,
    c_address,
    c_comment
having
    revenue is not null
order by
    revenue desc
""",
    ),
    8: StaticSQLVariant(
        8,
        "EXISTS pattern: Use EXISTS for return filtering",
        """\

select
    c_custkey,
    c_name,
    sum(l_extendedprice * (1 - l_discount)) as revenue,
    c_acctbal,
    n_name,
    c_address,
    c_phone,
    c_comment
from
    customer,
    orders,
    lineitem,
    nation
where
    c_custkey = o_custkey
    and l_orderkey = o_orderkey
    and o_orderdate >= date '1993-10-01'
    and o_orderdate < date '1993-10-01' + interval '3' month
    and c_nationkey = n_nationkey
    and exists (
        select 1
        from lineitem l2
        where l2.l_orderkey = l_orderkey
          and l2.l_linenumber = l_linenumber
          and l2.l_returnflag = 'R'
    )
group by
    c_custkey,
    c_name,
    c_acctbal,
    c_phone,
    n_name,
    c_address,
    c_comment
order by
    revenue desc
""",
    ),
    9: StaticSQLVariant(
        9,
        "Window functions: Use window functions for customer analysis (OLAP)",
        """\

select distinct
    c_custkey,
    c_name,
    sum(l_extendedprice * (1 - l_discount)) over (partition by c_custkey) as revenue,
    c_acctbal,
    n_name,
    c_address,
    c_phone,
    c_comment,
    rank() over (order by sum(l_extendedprice * (1 - l_discount)) over (partition by c_custkey) desc) as revenue_rank
from
    customer,
    orders,
    lineitem,
    nation
where
    c_custkey = o_custkey
    and l_orderkey = o_orderkey
    and o_orderdate >= date '1993-10-01'
    and o_orderdate < date '1993-10-01' + interval '3' month
    and l_returnflag = 'R'
    and c_nationkey = n_nationkey
order by
    revenue desc
""",
    ),
    10: StaticSQLVariant(
        10,
        "CASE expressions: Add extensive CASE logic for customer classification",
        """\

select
    c_custkey,
    case
        when c_name is not null then c_name
        else 'UNKNOWN CUSTOMER'
    end as c_name,
    sum(
        case
            when l_returnflag = 'R' then
                case
                    when l_discount between 0.05 and 0.07 then l_extendedprice * (1 - l_discount)
                    when l_discount < 0.05 then l_extendedprice * (1 - l_discount)
                    when l_discount > 0.07 then l_extendedprice * (1 - l_discount)
                    else l_extendedprice * (1 - l_discount)
                end
            else 0
        end
    ) as revenue,
    case
        when c_acctbal > 0 then c_acctbal
        else 0
    end as c_acctbal,
    case
        when n_name is not null then n_name
        else 'UNKNOWN NATION'
    end as n_name,
    c_address,
    c_phone,
    c_comment
from
    customer,
    orders,
    lineitem,
    nation
where
    c_custkey = o_custkey
    and l_orderkey = o_orderkey
    and case
        when o_orderdate >= date '1993-10-01' then 1
        else 0
    end = 1
    and case
        when o_orderdate < date '1993-10-01' + interval '3' month then 1
        else 0
    end = 1
    and c_nationkey = n_nationkey
group by
    c_custkey,
    c_name,
    c_acctbal,
    c_phone,
    n_name,
    c_address,
    c_comment
having
    sum(
        case
            when l_returnflag = 'R' then l_extendedprice * (1 - l_discount)
            else 0
        end
    ) > 0
order by
    revenue desc
""",
    ),
}

__all__ = ["VARIANTS"]
