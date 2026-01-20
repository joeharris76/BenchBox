"""Variant definitions for Query 11."""

from __future__ import annotations

from benchbox.core.tpchavoc.variant_base import StaticSQLVariant

VARIANTS = {
    1: StaticSQLVariant(
        1,
        "Scalar subqueries: Use scalar subqueries for threshold calculation",
        """\

select
    ps_partkey,
    sum(ps_supplycost * ps_availqty) as value
from
    partsupp,
    supplier,
    nation
where
    ps_suppkey = s_suppkey
    and s_nationkey = n_nationkey
    and n_name = 'GERMANY'
    and (ps_supplycost * ps_availqty) > (
        select avg(ps2.ps_supplycost * ps2.ps_availqty) * 0.0001
        from partsupp ps2, supplier s2, nation n2
        where ps2.ps_suppkey = s2.s_suppkey
          and s2.s_nationkey = n2.n_nationkey
          and n2.n_name = 'GERMANY'
          and ps2.ps_partkey = ps_partkey
    )
group by
    ps_partkey
having
    sum(ps_supplycost * ps_availqty) > (
        select sum(ps3.ps_supplycost * ps3.ps_availqty) * 0.0001
        from partsupp ps3, supplier s3, nation n3
        where ps3.ps_suppkey = s3.s_suppkey
          and s3.s_nationkey = n3.n_nationkey
          and n3.n_name = 'GERMANY'
    )
order by
    value desc
""",
    ),
    2: StaticSQLVariant(
        2,
        "HAVING clause: Enhanced HAVING with multiple conditions",
        """\

select
    ps_partkey,
    sum(ps_supplycost * ps_availqty) as value
from
    partsupp,
    supplier,
    nation
where
    ps_suppkey = s_suppkey
    and s_nationkey = n_nationkey
    and n_name = 'GERMANY'
group by
    ps_partkey
having
    sum(ps_supplycost * ps_availqty) > (
        select sum(ps_supplycost * ps_availqty) * 0.0001
        from partsupp, supplier, nation
        where ps_suppkey = s_suppkey
          and s_nationkey = n_nationkey
          and n_name = 'GERMANY'
    )
    and count(*) > 0
    and avg(ps_supplycost) > 0
order by
    value desc
""",
    ),
    3: StaticSQLVariant(
        3,
        "Explicit JOINs: Convert implicit joins to explicit JOIN syntax",
        """\

select
    ps.ps_partkey,
    sum(ps.ps_supplycost * ps.ps_availqty) as value
from
    partsupp ps
    inner join supplier s on ps.ps_suppkey = s.s_suppkey
    inner join nation n on s.s_nationkey = n.n_nationkey
where
    n.n_name = 'GERMANY'
group by
    ps.ps_partkey
having
    sum(ps.ps_supplycost * ps.ps_availqty) > (
        select sum(ps2.ps_supplycost * ps2.ps_availqty) * 0.0001
        from partsupp ps2
        inner join supplier s2 on ps2.ps_suppkey = s2.s_suppkey
        inner join nation n2 on s2.s_nationkey = n2.n_nationkey
        where n2.n_name = 'GERMANY'
    )
order by
    value desc
""",
    ),
    4: StaticSQLVariant(
        4,
        "UNION ALL: Split by supplier groups",
        """\

select
    ps_partkey,
    sum(value) as value
from (
    select
        ps_partkey,
        sum(ps_supplycost * ps_availqty) as value
    from partsupp, supplier, nation
    where ps_suppkey = s_suppkey
      and s_nationkey = n_nationkey
      and n_name = 'GERMANY'
      and s_suppkey % 2 = 0
    group by ps_partkey

    union all

    select
        ps_partkey,
        sum(ps_supplycost * ps_availqty) as value
    from partsupp, supplier, nation
    where ps_suppkey = s_suppkey
      and s_nationkey = n_nationkey
      and n_name = 'GERMANY'
      and s_suppkey % 2 = 1
    group by ps_partkey
) combined
group by ps_partkey
having
    sum(value) > (
        select sum(ps_supplycost * ps_availqty) * 0.0001
        from partsupp, supplier, nation
        where ps_suppkey = s_suppkey
          and s_nationkey = n_nationkey
          and n_name = 'GERMANY'
    )
order by
    value desc
""",
    ),
    5: StaticSQLVariant(
        5,
        "Common Table Expression: Use CTEs to break down threshold calculation",
        """\

with german_suppliers as (
    select s_suppkey
    from supplier, nation
    where s_nationkey = n_nationkey
      and n_name = 'GERMANY'
),
inventory_values as (
    select ps_partkey, ps_supplycost * ps_availqty as part_value
    from partsupp, german_suppliers
    where ps_suppkey = s_suppkey
),
total_threshold as (
    select sum(part_value) * 0.0001 as threshold
    from inventory_values
),
part_totals as (
    select ps_partkey, sum(part_value) as value
    from inventory_values
    group by ps_partkey
)
select
    pt.ps_partkey,
    pt.value
from part_totals pt, total_threshold tt
where pt.value > tt.threshold
order by pt.value desc
""",
    ),
    6: StaticSQLVariant(
        6,
        "Derived table: Wrap main query with derived table analysis",
        """\

select
    partkey,
    total_value
from (
    select
        ps_partkey as partkey,
        sum(ps_supplycost * ps_availqty) as total_value
    from
        partsupp,
        supplier,
        nation
    where
        ps_suppkey = s_suppkey
        and s_nationkey = n_nationkey
        and n_name = 'GERMANY'
    group by
        ps_partkey
    having
        sum(ps_supplycost * ps_availqty) > (
            select sum(ps_supplycost * ps_availqty) * 0.0001
            from partsupp, supplier, nation
            where ps_suppkey = s_suppkey
              and s_nationkey = n_nationkey
              and n_name = 'GERMANY'
        )
) inventory_summary
order by
    total_value desc
""",
    ),
    7: StaticSQLVariant(
        7,
        "FILTER clause: Use FILTER for conditional aggregation (OLAP)",
        """\

select
    ps_partkey,
    sum(ps_supplycost * ps_availqty)
        filter (where n_name = 'GERMANY') as value
from
    partsupp,
    supplier,
    nation
where
    ps_suppkey = s_suppkey
    and s_nationkey = n_nationkey
group by
    ps_partkey
having
    sum(ps_supplycost * ps_availqty) filter (where n_name = 'GERMANY') > (
        select sum(ps_supplycost * ps_availqty) * 0.0001
        from partsupp, supplier, nation
        where ps_suppkey = s_suppkey
          and s_nationkey = n_nationkey
          and n_name = 'GERMANY'
    )
    and value is not null
order by
    value desc
""",
    ),
    8: StaticSQLVariant(
        8,
        "EXISTS pattern: Use EXISTS for nation filtering",
        """\

select
    ps_partkey,
    sum(ps_supplycost * ps_availqty) as value
from
    partsupp,
    supplier
where
    ps_suppkey = s_suppkey
    and exists (
        select 1
        from nation
        where n_nationkey = s_nationkey
          and n_name = 'GERMANY'
    )
group by
    ps_partkey
having
    sum(ps_supplycost * ps_availqty) > (
        select sum(ps_supplycost * ps_availqty) * 0.0001
        from partsupp, supplier, nation
        where ps_suppkey = s_suppkey
          and s_nationkey = n_nationkey
          and n_name = 'GERMANY'
    )
order by
    value desc
""",
    ),
    9: StaticSQLVariant(
        9,
        "Window functions: Use window functions for inventory analysis (OLAP)",
        """\

select distinct
    ps_partkey,
    sum(ps_supplycost * ps_availqty) over (partition by ps_partkey) as value,
    rank() over (order by sum(ps_supplycost * ps_availqty) over (partition by ps_partkey) desc) as value_rank
from
    partsupp,
    supplier,
    nation
where
    ps_suppkey = s_suppkey
    and s_nationkey = n_nationkey
    and n_name = 'GERMANY'
    and sum(ps_supplycost * ps_availqty) over (partition by ps_partkey) > (
        select sum(ps_supplycost * ps_availqty) * 0.0001
        from partsupp, supplier, nation
        where ps_suppkey = s_suppkey
          and s_nationkey = n_nationkey
          and n_name = 'GERMANY'
    )
order by
    value desc
""",
    ),
    10: StaticSQLVariant(
        10,
        "CASE expressions: Add extensive CASE logic for inventory classification",
        """\

select
    ps_partkey,
    sum(
        case
            when ps_supplycost > 0 and ps_availqty > 0 then ps_supplycost * ps_availqty
            when ps_supplycost = 0 then 0
            when ps_availqty = 0 then 0
            else ps_supplycost * ps_availqty
        end
    ) as value
from
    partsupp,
    supplier,
    nation
where
    ps_suppkey = s_suppkey
    and s_nationkey = n_nationkey
    and case
        when n_name = 'GERMANY' then n_name
        else null
    end = 'GERMANY'
group by
    ps_partkey
having
    sum(
        case
            when ps_supplycost > 0 and ps_availqty > 0 then ps_supplycost * ps_availqty
            else 0
        end
    ) > (
        select
            case
                when sum(ps_supplycost * ps_availqty) > 0 then sum(ps_supplycost * ps_availqty) * 0.0001
                else 0
            end
        from partsupp, supplier, nation
        where ps_suppkey = s_suppkey
          and s_nationkey = n_nationkey
          and n_name = 'GERMANY'
    )
order by
    value desc
""",
    ),
}

__all__ = ["VARIANTS"]
