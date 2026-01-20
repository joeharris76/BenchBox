"""Variant definitions for Query 2."""

from __future__ import annotations

from benchbox.core.tpchavoc.variant_base import StaticSQLVariant

VARIANTS = {
    1: StaticSQLVariant(
        1,
        "Scalar subqueries: Use scalar subqueries for min cost calculation",
        """\

select
    s_acctbal,
    s_name,
    n_name,
    p_partkey,
    p_mfgr,
    s_address,
    s_phone,
    s_comment
from
    part,
    supplier,
    partsupp,
    nation,
    region
where
    p_partkey = ps_partkey
    and s_suppkey = ps_suppkey
    and p_size = 15
    and p_type like '%BRASS'
    and s_nationkey = n_nationkey
    and n_regionkey = r_regionkey
    and r_name = 'EUROPE'
    and ps_supplycost = (
        select min(ps_supplycost)
        from partsupp ps2, supplier s2, nation n2, region r2
        where p_partkey = ps2.ps_partkey
          and s2.s_suppkey = ps2.ps_suppkey
          and s2.s_nationkey = n2.n_nationkey
          and n2.n_regionkey = r2.r_regionkey
          and r2.r_name = 'EUROPE'
    )
order by
    s_acctbal desc,
    n_name,
    s_name,
    p_partkey
""",
    ),
    2: StaticSQLVariant(
        2,
        "HAVING clause: Add HAVING clause on cost aggregation",
        """\

select
    s_acctbal,
    s_name,
    n_name,
    p_partkey,
    p_mfgr,
    s_address,
    s_phone,
    s_comment
from
    part,
    supplier,
    partsupp,
    nation,
    region
where
    p_partkey = ps_partkey
    and s_suppkey = ps_suppkey
    and p_size = 15
    and p_type like '%BRASS'
    and s_nationkey = n_nationkey
    and n_regionkey = r_regionkey
    and r_name = 'EUROPE'
    and ps_supplycost = (
        select min(ps_supplycost)
        from partsupp, supplier, nation, region
        where p_partkey = ps_partkey
          and s_suppkey = ps_suppkey
          and s_nationkey = n_nationkey
          and n_regionkey = r_regionkey
          and r_name = 'EUROPE'
        group by ps_partkey
        having min(ps_supplycost) > 0
    )
order by
    s_acctbal desc,
    n_name,
    s_name,
    p_partkey
""",
    ),
    3: StaticSQLVariant(
        3,
        "Explicit JOINs: Use explicit JOIN syntax for all relationships",
        """\

select
    s_acctbal,
    s_name,
    n_name,
    p_partkey,
    p_mfgr,
    s_address,
    s_phone,
    s_comment
from
    part p
    inner join partsupp ps on p.p_partkey = ps.ps_partkey
    inner join supplier s on s.s_suppkey = ps.ps_suppkey
    inner join nation n on s.s_nationkey = n.n_nationkey
    inner join region r on n.n_regionkey = r.r_regionkey
where
    p.p_size = 15
    and p.p_type like '%BRASS'
    and r.r_name = 'EUROPE'
    and ps.ps_supplycost = (
        select min(ps2.ps_supplycost)
        from partsupp ps2
        inner join supplier s2 on s2.s_suppkey = ps2.ps_suppkey
        inner join nation n2 on s2.s_nationkey = n2.n_nationkey
        inner join region r2 on n2.n_regionkey = r2.r_regionkey
        where p.p_partkey = ps2.ps_partkey
          and r2.r_name = 'EUROPE'
    )
order by
    s.s_acctbal desc,
    n.n_name,
    s.s_name,
    p.p_partkey
""",
    ),
    4: StaticSQLVariant(
        4,
        "UNION ALL: Split by region combinations",
        """\

select
    s_acctbal,
    s_name,
    n_name,
    p_partkey,
    p_mfgr,
    s_address,
    s_phone,
    s_comment
from (
    select
        s_acctbal,
        s_name,
        n_name,
        p_partkey,
        p_mfgr,
        s_address,
        s_phone,
        s_comment,
        ps_supplycost
    from
        part,
        supplier,
        partsupp,
        nation,
        region
    where
        p_partkey = ps_partkey
        and s_suppkey = ps_suppkey
        and p_size = 15
        and p_type like '%BRASS'
        and s_nationkey = n_nationkey
        and n_regionkey = r_regionkey
        and r_name = 'EUROPE'

    union all

    select
        null as s_acctbal,
        null as s_name,
        null as n_name,
        p_partkey,
        null as p_mfgr,
        null as s_address,
        null as s_phone,
        null as s_comment,
        min(ps_supplycost) as ps_supplycost
    from
        part,
        supplier,
        partsupp,
        nation,
        region
    where
        p_partkey = ps_partkey
        and s_suppkey = ps_suppkey
        and p_size = 15
        and p_type like '%BRASS'
        and s_nationkey = n_nationkey
        and n_regionkey = r_regionkey
        and r_name = 'EUROPE'
    group by p_partkey
) combined
where combined.s_acctbal is not null
  and combined.ps_supplycost = (
    select min(c2.ps_supplycost)
    from (
        select p_partkey, min(ps_supplycost) as ps_supplycost
        from part, supplier, partsupp, nation, region
        where p_partkey = ps_partkey
          and s_suppkey = ps_suppkey
          and p_size = 15
          and p_type like '%BRASS'
          and s_nationkey = n_nationkey
          and n_regionkey = r_regionkey
          and r_name = 'EUROPE'
        group by p_partkey
    ) c2
    where c2.p_partkey = combined.p_partkey
  )
order by
    s_acctbal desc,
    n_name,
    s_name,
    p_partkey
""",
    ),
    5: StaticSQLVariant(
        5,
        "CTE: Pre-compute minimum costs",
        """\

with min_costs as (
    select
        ps_partkey,
        min(ps_supplycost) as min_cost
    from
        partsupp,
        supplier,
        nation,
        region
    where
        s_suppkey = ps_suppkey
        and s_nationkey = n_nationkey
        and n_regionkey = r_regionkey
        and r_name = 'EUROPE'
    group by
        ps_partkey
)
select
    s_acctbal,
    s_name,
    n_name,
    p_partkey,
    p_mfgr,
    s_address,
    s_phone,
    s_comment
from
    part,
    supplier,
    partsupp,
    nation,
    region,
    min_costs mc
where
    p_partkey = ps_partkey
    and s_suppkey = ps_suppkey
    and p_size = 15
    and p_type like '%BRASS'
    and s_nationkey = n_nationkey
    and n_regionkey = r_regionkey
    and r_name = 'EUROPE'
    and p_partkey = mc.ps_partkey
    and ps_supplycost = mc.min_cost
order by
    s_acctbal desc,
    n_name,
    s_name,
    p_partkey
""",
    ),
    6: StaticSQLVariant(
        6,
        "Derived table: Materialize cost calculations",
        """\

select
    results.s_acctbal,
    results.s_name,
    results.n_name,
    results.p_partkey,
    results.p_mfgr,
    results.s_address,
    results.s_phone,
    results.s_comment
from (
    select
        s_acctbal,
        s_name,
        n_name,
        p_partkey,
        p_mfgr,
        s_address,
        s_phone,
        s_comment,
        ps_supplycost
    from
        part,
        supplier,
        partsupp,
        nation,
        region
    where
        p_partkey = ps_partkey
        and s_suppkey = ps_suppkey
        and p_size = 15
        and p_type like '%BRASS'
        and s_nationkey = n_nationkey
        and n_regionkey = r_regionkey
        and r_name = 'EUROPE'
) results
where results.ps_supplycost = (
    select min(ps_supplycost)
    from partsupp, supplier, nation, region
    where results.p_partkey = ps_partkey
      and s_suppkey = ps_suppkey
      and s_nationkey = n_nationkey
      and n_regionkey = r_regionkey
      and r_name = 'EUROPE'
)
order by
    results.s_acctbal desc,
    results.n_name,
    results.s_name,
    results.p_partkey
""",
    ),
    7: StaticSQLVariant(
        7,
        "FILTER clause: Use FILTER for conditional aggregation",
        """\

with cost_analysis as (
    select
        p_partkey,
        s_suppkey,
        s_acctbal,
        s_name,
        n_name,
        p_mfgr,
        s_address,
        s_phone,
        s_comment,
        ps_supplycost,
        min(ps_supplycost) filter (where r_name = 'EUROPE') over (partition by p_partkey) as min_cost
    from
        part,
        supplier,
        partsupp,
        nation,
        region
    where
        p_partkey = ps_partkey
        and s_suppkey = ps_suppkey
        and p_size = 15
        and p_type like '%BRASS'
        and s_nationkey = n_nationkey
        and n_regionkey = r_regionkey
        and r_name = 'EUROPE'
)
select
    s_acctbal,
    s_name,
    n_name,
    p_partkey,
    p_mfgr,
    s_address,
    s_phone,
    s_comment
from cost_analysis
where ps_supplycost = min_cost
order by
    s_acctbal desc,
    n_name,
    s_name,
    p_partkey
""",
    ),
    8: StaticSQLVariant(
        8,
        "EXISTS: Convert correlated subquery to EXISTS",
        """\

select
    s_acctbal,
    s_name,
    n_name,
    p_partkey,
    p_mfgr,
    s_address,
    s_phone,
    s_comment
from
    part,
    supplier,
    partsupp,
    nation,
    region
where
    p_partkey = ps_partkey
    and s_suppkey = ps_suppkey
    and p_size = 15
    and p_type like '%BRASS'
    and s_nationkey = n_nationkey
    and n_regionkey = r_regionkey
    and r_name = 'EUROPE'
    and not exists (
        select 1
        from partsupp ps2, supplier s2, nation n2, region r2
        where p_partkey = ps2.ps_partkey
          and s2.s_suppkey = ps2.ps_suppkey
          and s2.s_nationkey = n2.n_nationkey
          and n2.n_regionkey = r2.r_regionkey
          and r2.r_name = 'EUROPE'
          and ps2.ps_supplycost < ps_supplycost
    )
order by
    s_acctbal desc,
    n_name,
    s_name,
    p_partkey
""",
    ),
    9: StaticSQLVariant(
        9,
        "Window functions: Use RANK() for minimum selection",
        """\

with ranked_suppliers as (
    select
        s_acctbal,
        s_name,
        n_name,
        p_partkey,
        p_mfgr,
        s_address,
        s_phone,
        s_comment,
        ps_supplycost,
        rank() over (partition by p_partkey order by ps_supplycost) as cost_rank
    from
        part,
        supplier,
        partsupp,
        nation,
        region
    where
        p_partkey = ps_partkey
        and s_suppkey = ps_suppkey
        and p_size = 15
        and p_type like '%BRASS'
        and s_nationkey = n_nationkey
        and n_regionkey = r_regionkey
        and r_name = 'EUROPE'
)
select
    s_acctbal,
    s_name,
    n_name,
    p_partkey,
    p_mfgr,
    s_address,
    s_phone,
    s_comment
from ranked_suppliers
where cost_rank = 1
order by
    s_acctbal desc,
    n_name,
    s_name,
    p_partkey
""",
    ),
    10: StaticSQLVariant(
        10,
        "CASE expressions: Add conditional logic for cost selection",
        """\

select
    case when s_acctbal > 0 then s_acctbal else 0 end as s_acctbal,
    case when s_name is not null then s_name else 'UNKNOWN' end as s_name,
    case when n_name is not null then n_name else 'UNKNOWN' end as n_name,
    p_partkey,
    case when p_mfgr is not null then p_mfgr else 'UNKNOWN' end as p_mfgr,
    case when s_address is not null then s_address else 'UNKNOWN' end as s_address,
    case when s_phone is not null then s_phone else 'UNKNOWN' end as s_phone,
    case when s_comment is not null then s_comment else '' end as s_comment
from
    part,
    supplier,
    partsupp,
    nation,
    region
where
    p_partkey = ps_partkey
    and s_suppkey = ps_suppkey
    and p_size = case when 15 > 0 then 15 else 1 end
    and p_type like case when '%BRASS' is not null then '%BRASS' else '%' end
    and s_nationkey = n_nationkey
    and n_regionkey = r_regionkey
    and r_name = case when 'EUROPE' is not null then 'EUROPE' else r_name end
    and ps_supplycost = (
        select min(case when ps_supplycost > 0 then ps_supplycost else 999999 end)
        from partsupp, supplier, nation, region
        where p_partkey = ps_partkey
          and s_suppkey = ps_suppkey
          and s_nationkey = n_nationkey
          and n_regionkey = r_regionkey
          and r_name = 'EUROPE'
    )
order by
    case when s_acctbal > 0 then s_acctbal else 0 end desc,
    case when n_name is not null then n_name else 'ZZZZZ' end,
    case when s_name is not null then s_name else 'ZZZZZ' end,
    p_partkey
""",
    ),
}

__all__ = ["VARIANTS"]
