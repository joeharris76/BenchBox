"""Variant definitions for Query 1."""

from __future__ import annotations

from benchbox.core.tpchavoc.variant_base import StaticSQLVariant

VARIANTS = {
    1: StaticSQLVariant(
        1,
        "Scalar subqueries: Use scalar subqueries for aggregation calculations",
        """\

select
    l_returnflag,
    l_linestatus,
    sum(l_quantity) as sum_qty,
    sum(l_extendedprice) as sum_base_price,
    sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
    sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
    (select avg(l2.l_quantity)
     from lineitem l2
     where l2.l_shipdate <= date '1998-12-01' - interval '90' day
       and l2.l_returnflag = l1.l_returnflag
       and l2.l_linestatus = l1.l_linestatus) as avg_qty,
    (select avg(l2.l_extendedprice)
     from lineitem l2
     where l2.l_shipdate <= date '1998-12-01' - interval '90' day
       and l2.l_returnflag = l1.l_returnflag
       and l2.l_linestatus = l1.l_linestatus) as avg_price,
    avg(l_discount) as avg_disc,
    count(*) as count_order
from
    lineitem l1
where
    l_shipdate <= date '1998-12-01' - interval '90' day
group by
    l_returnflag,
    l_linestatus
order by
    l_returnflag,
    l_linestatus
""",
    ),
    2: StaticSQLVariant(
        2,
        "HAVING clause: Use HAVING for post-aggregation filtering",
        """\

select
    l_returnflag,
    l_linestatus,
    sum(l_quantity) as sum_qty,
    sum(l_extendedprice) as sum_base_price,
    sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
    sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
    avg(l_quantity) as avg_qty,
    avg(l_extendedprice) as avg_price,
    avg(l_discount) as avg_disc,
    count(*) as count_order
from
    lineitem
where
    l_shipdate <= date '1998-12-01' - interval '90' day
group by
    l_returnflag,
    l_linestatus
having
    count(*) > 0
    and sum(l_quantity) > 0
    and avg(l_discount) between 0 and 1
order by
    l_returnflag,
    l_linestatus
""",
    ),
    3: StaticSQLVariant(
        3,
        "Multiple FROM clauses: Use explicit join syntax with same table",
        """\

select
    l1.l_returnflag,
    l1.l_linestatus,
    sum(l1.l_quantity) as sum_qty,
    sum(l1.l_extendedprice) as sum_base_price,
    sum(l1.l_extendedprice * (1 - l1.l_discount)) as sum_disc_price,
    sum(l1.l_extendedprice * (1 - l1.l_discount) * (1 + l1.l_tax)) as sum_charge,
    avg(l1.l_quantity) as avg_qty,
    avg(l1.l_extendedprice) as avg_price,
    avg(l1.l_discount) as avg_disc,
    count(*) as count_order
from
    lineitem l1
    inner join (select distinct l_returnflag, l_linestatus
                from lineitem
                where l_shipdate <= date '1998-12-01' - interval '90' day) l2
    on l1.l_returnflag = l2.l_returnflag and l1.l_linestatus = l2.l_linestatus
where
    l1.l_shipdate <= date '1998-12-01' - interval '90' day
group by
    l1.l_returnflag,
    l1.l_linestatus
order by
    l1.l_returnflag,
    l1.l_linestatus
""",
    ),
    4: StaticSQLVariant(
        4,
        "UNION ALL approach: Combine separate aggregation queries",
        """\

select
    final.l_returnflag,
    final.l_linestatus,
    final.sum_qty,
    final.sum_base_price,
    final.sum_disc_price,
    final.sum_charge,
    final.sum_qty / final.count_order as avg_qty,
    final.sum_base_price / final.count_order as avg_price,
    final.sum_disc / final.count_order as avg_disc,
    final.count_order
from (
    select
        l_returnflag,
        l_linestatus,
        sum(l_quantity) as sum_qty,
        sum(l_extendedprice) as sum_base_price,
        sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
        sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
        sum(l_discount) as sum_disc,
        count(*) as count_order
    from lineitem
    where l_shipdate <= date '1998-12-01' - interval '90' day
    group by l_returnflag, l_linestatus

    union all

    select
        l_returnflag,
        l_linestatus,
        0 as sum_qty,
        0 as sum_base_price,
        0 as sum_disc_price,
        0 as sum_charge,
        0 as sum_disc,
        0 as count_order
    from lineitem
    where l_shipdate <= date '1998-12-01' - interval '90' day
    group by l_returnflag, l_linestatus
    having 1=0  -- Empty result to test UNION ALL handling
) final
where final.count_order > 0
order by final.l_returnflag, final.l_linestatus
""",
    ),
    5: StaticSQLVariant(
        5,
        "Common Table Expression: Use WITH clause for readable structure",
        """\

with filtered_lineitem as (
    select
        l_returnflag,
        l_linestatus,
        l_quantity,
        l_extendedprice,
        l_discount,
        l_tax
    from
        lineitem
    where
        l_shipdate <= date '1998-12-01' - interval '90' day
)
select
    l_returnflag,
    l_linestatus,
    sum(l_quantity) as sum_qty,
    sum(l_extendedprice) as sum_base_price,
    sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
    sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
    avg(l_quantity) as avg_qty,
    avg(l_extendedprice) as avg_price,
    avg(l_discount) as avg_disc,
    count(*) as count_order
from
    filtered_lineitem
group by
    l_returnflag,
    l_linestatus
order by
    l_returnflag,
    l_linestatus
""",
    ),
    6: StaticSQLVariant(
        6,
        "FILTER clause: Use FILTER clause for conditional aggregation",
        """\

select
    l_returnflag,
    l_linestatus,
    sum(l_quantity) filter (where l_shipdate <= date '1998-12-01' - interval '90' day) as sum_qty,
    sum(l_extendedprice) filter (where l_shipdate <= date '1998-12-01' - interval '90' day) as sum_base_price,
    sum(l_extendedprice * (1 - l_discount)) filter (where l_shipdate <= date '1998-12-01' - interval '90' day) as sum_disc_price,
    sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) filter (where l_shipdate <= date '1998-12-01' - interval '90' day) as sum_charge,
    avg(l_quantity) filter (where l_shipdate <= date '1998-12-01' - interval '90' day) as avg_qty,
    avg(l_extendedprice) filter (where l_shipdate <= date '1998-12-01' - interval '90' day) as avg_price,
    avg(l_discount) filter (where l_shipdate <= date '1998-12-01' - interval '90' day) as avg_disc,
    count(*) filter (where l_shipdate <= date '1998-12-01' - interval '90' day) as count_order
from
    lineitem
group by
    l_returnflag,
    l_linestatus
order by
    l_returnflag,
    l_linestatus
""",
    ),
    7: StaticSQLVariant(
        7,
        "Array aggregation: Use array functions for data collection",
        """\

with aggregated as (
    select
        l_returnflag,
        l_linestatus,
        list(l_quantity) as quantities,
        list(l_extendedprice) as prices,
        list(l_discount) as discounts,
        list(l_tax) as taxes
    from
        lineitem
    where
        l_shipdate <= date '1998-12-01' - interval '90' day
    group by
        l_returnflag,
        l_linestatus
)
select
    l_returnflag,
    l_linestatus,
    list_sum(quantities) as sum_qty,
    list_sum(prices) as sum_base_price,
    list_sum(list_transform(list_zip(prices, discounts), x -> x[1] * (1 - x[2]))) as sum_disc_price,
    list_sum(list_transform(list_zip(prices, discounts, taxes), x -> x[1] * (1 - x[2]) * (1 + x[3]))) as sum_charge,
    list_avg(quantities) as avg_qty,
    list_avg(prices) as avg_price,
    list_avg(discounts) as avg_disc,
    len(quantities) as count_order
from
    aggregated
order by
    l_returnflag,
    l_linestatus
""",
    ),
    8: StaticSQLVariant(
        8,
        "QUALIFY clause: Use QUALIFY to filter window function results",
        """\

select distinct
    l_returnflag,
    l_linestatus,
    sum(l_quantity) over w as sum_qty,
    sum(l_extendedprice) over w as sum_base_price,
    sum(l_extendedprice * (1 - l_discount)) over w as sum_disc_price,
    sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) over w as sum_charge,
    avg(l_quantity) over w as avg_qty,
    avg(l_extendedprice) over w as avg_price,
    avg(l_discount) over w as avg_disc,
    count(*) over w as count_order
from
    lineitem
window w as (partition by l_returnflag, l_linestatus)
qualify count(*) over w > 0
    and l_shipdate <= date '1998-12-01' - interval '90' day
order by
    l_returnflag,
    l_linestatus
""",
    ),
    9: StaticSQLVariant(
        9,
        "GROUPING SETS: Use GROUPING SETS for hierarchical aggregation",
        """\

select
    l_returnflag,
    l_linestatus,
    sum(l_quantity) as sum_qty,
    sum(l_extendedprice) as sum_base_price,
    sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
    sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
    avg(l_quantity) as avg_qty,
    avg(l_extendedprice) as avg_price,
    avg(l_discount) as avg_disc,
    count(*) as count_order
from
    lineitem
where
    l_shipdate <= date '1998-12-01' - interval '90' day
group by grouping sets (
    (l_returnflag, l_linestatus)
)
having
    l_returnflag is not null
    and l_linestatus is not null
order by
    l_returnflag,
    l_linestatus
""",
    ),
    10: StaticSQLVariant(
        10,
        "CASE expressions: Use CASE for conditional logic",
        """\

select
    case
        when l_returnflag = 'A' then 'A'
        when l_returnflag = 'N' then 'N'
        when l_returnflag = 'R' then 'R'
        else l_returnflag
    end as l_returnflag,
    case
        when l_linestatus = 'F' then 'F'
        when l_linestatus = 'O' then 'O'
        else l_linestatus
    end as l_linestatus,
    sum(case when l_quantity is not null then l_quantity else 0 end) as sum_qty,
    sum(case when l_extendedprice is not null then l_extendedprice else 0 end) as sum_base_price,
    sum(case when l_extendedprice is not null and l_discount is not null
             then l_extendedprice * (1 - l_discount) else 0 end) as sum_disc_price,
    sum(case when l_extendedprice is not null and l_discount is not null and l_tax is not null
             then l_extendedprice * (1 - l_discount) * (1 + l_tax) else 0 end) as sum_charge,
    avg(case when l_quantity is not null then l_quantity end) as avg_qty,
    avg(case when l_extendedprice is not null then l_extendedprice end) as avg_price,
    avg(case when l_discount is not null then l_discount end) as avg_disc,
    count(case when l_returnflag is not null and l_linestatus is not null then 1 end) as count_order
from
    lineitem
where
    l_shipdate <= case when '1998-12-01' is not null
                       then date '1998-12-01' - interval '90' day
                       else date '1998-09-02' end
group by
    case
        when l_returnflag = 'A' then 'A'
        when l_returnflag = 'N' then 'N'
        when l_returnflag = 'R' then 'R'
        else l_returnflag
    end,
    case
        when l_linestatus = 'F' then 'F'
        when l_linestatus = 'O' then 'O'
        else l_linestatus
    end
order by
    l_returnflag,
    l_linestatus
""",
    ),
}

__all__ = ["VARIANTS"]
