"""Variant definitions for Query 8."""

from __future__ import annotations

from benchbox.core.tpchavoc.variant_base import StaticSQLVariant

VARIANTS = {
    1: StaticSQLVariant(
        1,
        "Scalar subqueries: Use scalar subqueries for market share calculation",
        """\

select
    o_year,
    (select sum(case when nation = 'BRAZIL' then volume else 0 end)
     from (
         select
             extract(year from o2.o_orderdate) as o_year2,
             l2.l_extendedprice * (1 - l2.l_discount) as volume,
             n3.n_name as nation
         from part p2, supplier s2, lineitem l2, orders o2, customer c2, nation n2, nation n3, region r2
         where p2.p_partkey = l2.l_partkey
           and s2.s_suppkey = l2.l_suppkey
           and l2.l_orderkey = o2.o_orderkey
           and o2.o_custkey = c2.c_custkey
           and c2.c_nationkey = n2.n_nationkey
           and n2.n_regionkey = r2.r_regionkey
           and r2.r_name = 'AMERICA'
           and s2.s_nationkey = n3.n_nationkey
           and o2.o_orderdate between date '1995-01-01' and date '1996-12-31'
           and p2.p_type = 'ECONOMY ANODIZED STEEL'
           and extract(year from o2.o_orderdate) = all_nations.o_year
     ) subq) /
    (select sum(volume)
     from (
         select
             extract(year from o3.o_orderdate) as o_year3,
             l3.l_extendedprice * (1 - l3.l_discount) as volume
         from part p3, supplier s3, lineitem l3, orders o3, customer c3, nation n4, nation n5, region r3
         where p3.p_partkey = l3.l_partkey
           and s3.s_suppkey = l3.l_suppkey
           and l3.l_orderkey = o3.o_orderkey
           and o3.o_custkey = c3.c_custkey
           and c3.c_nationkey = n4.n_nationkey
           and n4.n_regionkey = r3.r_regionkey
           and r3.r_name = 'AMERICA'
           and s3.s_nationkey = n5.n_nationkey
           and o3.o_orderdate between date '1995-01-01' and date '1996-12-31'
           and p3.p_type = 'ECONOMY ANODIZED STEEL'
           and extract(year from o3.o_orderdate) = all_nations.o_year
     ) subq2) as mkt_share
from (
    select distinct extract(year from o_orderdate) as o_year
    from part, supplier, lineitem, orders, customer, nation n1, nation n2, region
    where p_partkey = l_partkey
      and s_suppkey = l_suppkey
      and l_orderkey = o_orderkey
      and o_custkey = c_custkey
      and c_nationkey = n1.n_nationkey
      and n1.n_regionkey = r_regionkey
      and r_name = 'AMERICA'
      and s_nationkey = n2.n_nationkey
      and o_orderdate between date '1995-01-01' and date '1996-12-31'
      and p_type = 'ECONOMY ANODIZED STEEL'
) as all_nations
group by o_year
order by o_year
""",
    ),
    2: StaticSQLVariant(
        2,
        "HAVING clause: Use HAVING for market share filtering",
        """\

select
    o_year,
    sum(case when nation = 'BRAZIL' then volume else 0 end) / sum(volume) as mkt_share
from (
    select
        extract(year from o_orderdate) as o_year,
        l_extendedprice * (1 - l_discount) as volume,
        n2.n_name as nation
    from
        part, supplier, lineitem, orders, customer, nation n1, nation n2, region
    where
        p_partkey = l_partkey
        and s_suppkey = l_suppkey
        and l_orderkey = o_orderkey
        and o_custkey = c_custkey
        and c_nationkey = n1.n_nationkey
        and n1.n_regionkey = r_regionkey
        and r_name = 'AMERICA'
        and s_nationkey = n2.n_nationkey
        and o_orderdate between date '1995-01-01' and date '1996-12-31'
        and p_type = 'ECONOMY ANODIZED STEEL'
) as all_nations
group by
    o_year
having
    sum(volume) > 0
order by
    o_year
""",
    ),
    3: StaticSQLVariant(
        3,
        "Explicit JOINs: Convert all implicit joins to explicit JOIN syntax",
        """\

select
    o_year,
    sum(case when nation = 'BRAZIL' then volume else 0 end) / sum(volume) as mkt_share
from (
    select
        extract(year from o.o_orderdate) as o_year,
        l.l_extendedprice * (1 - l.l_discount) as volume,
        n2.n_name as nation
    from
        part p
        inner join lineitem l on p.p_partkey = l.l_partkey
        inner join supplier s on l.l_suppkey = s.s_suppkey
        inner join orders o on l.l_orderkey = o.o_orderkey
        inner join customer c on o.o_custkey = c.c_custkey
        inner join nation n1 on c.c_nationkey = n1.n_nationkey
        inner join region r on n1.n_regionkey = r.r_regionkey
        inner join nation n2 on s.s_nationkey = n2.n_nationkey
    where
        r.r_name = 'AMERICA'
        and o.o_orderdate between date '1995-01-01' and date '1996-12-31'
        and p.p_type = 'ECONOMY ANODIZED STEEL'
) as all_nations
group by
    o_year
order by
    o_year
""",
    ),
    4: StaticSQLVariant(
        4,
        "UNION ALL: Split by year ranges",
        """\

select
    o_year,
    sum(case when nation = 'BRAZIL' then volume else 0 end) / sum(volume) as mkt_share
from (
    select
        extract(year from o_orderdate) as o_year,
        l_extendedprice * (1 - l_discount) as volume,
        n2.n_name as nation
    from part, supplier, lineitem, orders, customer, nation n1, nation n2, region
    where p_partkey = l_partkey
      and s_suppkey = l_suppkey
      and l_orderkey = o_orderkey
      and o_custkey = c_custkey
      and c_nationkey = n1.n_nationkey
      and n1.n_regionkey = r_regionkey
      and r_name = 'AMERICA'
      and s_nationkey = n2.n_nationkey
      and o_orderdate between date '1995-01-01' and date '1995-12-31'
      and p_type = 'ECONOMY ANODIZED STEEL'

    union all

    select
        extract(year from o_orderdate) as o_year,
        l_extendedprice * (1 - l_discount) as volume,
        n2.n_name as nation
    from part, supplier, lineitem, orders, customer, nation n1, nation n2, region
    where p_partkey = l_partkey
      and s_suppkey = l_suppkey
      and l_orderkey = o_orderkey
      and o_custkey = c_custkey
      and c_nationkey = n1.n_nationkey
      and n1.n_regionkey = r_regionkey
      and r_name = 'AMERICA'
      and s_nationkey = n2.n_nationkey
      and o_orderdate between date '1996-01-01' and date '1996-12-31'
      and p_type = 'ECONOMY ANODIZED STEEL'
) as all_nations
group by
    o_year
order by
    o_year
""",
    ),
    5: StaticSQLVariant(
        5,
        "Common Table Expression: Use CTEs to break down complex joins",
        """\

with america_region as (
    select r_regionkey
    from region
    where r_name = 'AMERICA'
),
america_nations as (
    select n_nationkey, n_name
    from nation, america_region
    where n_regionkey = r_regionkey
),
qualified_customers as (
    select c_custkey, c_nationkey
    from customer, america_nations
    where c_nationkey = n_nationkey
),
qualified_orders as (
    select o_orderkey, o_custkey, o_orderdate
    from orders
    where o_orderdate between date '1995-01-01' and date '1996-12-31'
),
qualified_parts as (
    select p_partkey
    from part
    where p_type = 'ECONOMY ANODIZED STEEL'
),
qualified_lineitems as (
    select l_orderkey, l_partkey, l_suppkey, l_extendedprice, l_discount
    from lineitem, qualified_parts
    where l_partkey = p_partkey
),
market_data as (
    select
        extract(year from qo.o_orderdate) as o_year,
        ql.l_extendedprice * (1 - ql.l_discount) as volume,
        n.n_name as nation
    from
        qualified_lineitems ql
        join qualified_orders qo on ql.l_orderkey = qo.o_orderkey
        join qualified_customers qc on qo.o_custkey = qc.c_custkey
        join supplier s on ql.l_suppkey = s.s_suppkey
        join nation n on s.s_nationkey = n.n_nationkey
)
select
    o_year,
    sum(case when nation = 'BRAZIL' then volume else 0 end) / sum(volume) as mkt_share
from market_data
group by o_year
order by o_year
""",
    ),
    6: StaticSQLVariant(
        6,
        "Nested derived tables: Multiple levels of derived tables",
        """\

select
    o_year,
    market_share
from (
    select
        o_year,
        sum(case when nation = 'BRAZIL' then volume else 0 end) / sum(volume) as market_share
    from (
        select
            market_info.o_year,
            market_info.volume,
            market_info.nation
        from (
            select
                extract(year from o_orderdate) as o_year,
                l_extendedprice * (1 - l_discount) as volume,
                n2.n_name as nation
            from
                part, supplier, lineitem, orders, customer, nation n1, nation n2, region
            where
                p_partkey = l_partkey
                and s_suppkey = l_suppkey
                and l_orderkey = o_orderkey
                and o_custkey = c_custkey
                and c_nationkey = n1.n_nationkey
                and n1.n_regionkey = r_regionkey
                and r_name = 'AMERICA'
                and s_nationkey = n2.n_nationkey
                and o_orderdate between date '1995-01-01' and date '1996-12-31'
                and p_type = 'ECONOMY ANODIZED STEEL'
        ) as market_info
    ) as market_details
    group by o_year
) as market_summary
order by o_year
""",
    ),
    7: StaticSQLVariant(
        7,
        "FILTER clause: Use FILTER for conditional aggregation (OLAP)",
        """\

select
    o_year,
    sum(volume) filter (where nation = 'BRAZIL') / sum(volume) as mkt_share
from (
    select
        extract(year from o_orderdate) as o_year,
        l_extendedprice * (1 - l_discount) as volume,
        n2.n_name as nation
    from
        part, supplier, lineitem, orders, customer, nation n1, nation n2, region
    where
        p_partkey = l_partkey
        and s_suppkey = l_suppkey
        and l_orderkey = o_orderkey
        and o_custkey = c_custkey
        and c_nationkey = n1.n_nationkey
        and n1.n_regionkey = r_regionkey
        and r_name = 'AMERICA'
        and s_nationkey = n2.n_nationkey
        and o_orderdate between date '1995-01-01' and date '1996-12-31'
        and p_type = 'ECONOMY ANODIZED STEEL'
) as all_nations
group by
    o_year
order by
    o_year
""",
    ),
    8: StaticSQLVariant(
        8,
        "EXISTS pattern: Use EXISTS for region filtering",
        """\

select
    o_year,
    sum(case when nation = 'BRAZIL' then volume else 0 end) / sum(volume) as mkt_share
from (
    select
        extract(year from o_orderdate) as o_year,
        l_extendedprice * (1 - l_discount) as volume,
        n2.n_name as nation
    from
        part, supplier, lineitem, orders, customer, nation n1, nation n2
    where
        p_partkey = l_partkey
        and s_suppkey = l_suppkey
        and l_orderkey = o_orderkey
        and o_custkey = c_custkey
        and c_nationkey = n1.n_nationkey
        and s_nationkey = n2.n_nationkey
        and o_orderdate between date '1995-01-01' and date '1996-12-31'
        and p_type = 'ECONOMY ANODIZED STEEL'
        and exists (
            select 1
            from region
            where r_regionkey = n1.n_regionkey
              and r_name = 'AMERICA'
        )
) as all_nations
group by
    o_year
order by
    o_year
""",
    ),
    9: StaticSQLVariant(
        9,
        "Window functions: Use window functions for market share analysis (OLAP)",
        """\

select distinct
    o_year,
    sum(case when nation = 'BRAZIL' then volume else 0 end) over (partition by o_year) /
    sum(volume) over (partition by o_year) as mkt_share,
    rank() over (order by o_year) as year_rank
from (
    select
        extract(year from o_orderdate) as o_year,
        l_extendedprice * (1 - l_discount) as volume,
        n2.n_name as nation
    from
        part, supplier, lineitem, orders, customer, nation n1, nation n2, region
    where
        p_partkey = l_partkey
        and s_suppkey = l_suppkey
        and l_orderkey = o_orderkey
        and o_custkey = c_custkey
        and c_nationkey = n1.n_nationkey
        and n1.n_regionkey = r_regionkey
        and r_name = 'AMERICA'
        and s_nationkey = n2.n_nationkey
        and o_orderdate between date '1995-01-01' and date '1996-12-31'
        and p_type = 'ECONOMY ANODIZED STEEL'
) as all_nations
order by
    o_year
""",
    ),
    10: StaticSQLVariant(
        10,
        "CASE expressions: Add extensive CASE logic for conditional processing",
        """\

select
    o_year,
    sum(
        case
            when nation = 'BRAZIL' then
                case
                    when volume > 0 then volume
                    else 0
                end
            else 0
        end
    ) / sum(
        case
            when volume > 0 then volume
            else 0
        end
    ) as mkt_share
from (
    select
        case
            when extract(year from o_orderdate) between 1995 and 1996 then extract(year from o_orderdate)
            else null
        end as o_year,
        case
            when l_discount between 0.05 and 0.07 then l_extendedprice * (1 - l_discount)
            when l_discount < 0.05 then l_extendedprice * (1 - l_discount)
            when l_discount > 0.07 then l_extendedprice * (1 - l_discount)
            else l_extendedprice * (1 - l_discount)
        end as volume,
        case
            when n2.n_name in ('BRAZIL', 'ARGENTINA', 'CANADA', 'PERU', 'UNITED STATES') then n2.n_name
            else 'OTHER'
        end as nation
    from
        part, supplier, lineitem, orders, customer, nation n1, nation n2, region
    where
        p_partkey = l_partkey
        and s_suppkey = l_suppkey
        and l_orderkey = o_orderkey
        and o_custkey = c_custkey
        and c_nationkey = n1.n_nationkey
        and n1.n_regionkey = r_regionkey
        and s_nationkey = n2.n_nationkey
        and case
            when r_name = 'AMERICA' then r_name
            else null
        end = 'AMERICA'
        and case
            when o_orderdate >= date '1995-01-01' then 1
            else 0
        end = 1
        and case
            when o_orderdate <= date '1996-12-31' then 1
            else 0
        end = 1
        and case
            when p_type = 'ECONOMY ANODIZED STEEL' then p_type
            else null
        end = 'ECONOMY ANODIZED STEEL'
) as all_nations
where o_year is not null
group by
    o_year
order by
    o_year
""",
    ),
}

__all__ = ["VARIANTS"]
