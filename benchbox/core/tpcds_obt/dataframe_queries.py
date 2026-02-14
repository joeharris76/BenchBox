"""TPC-DS-OBT DataFrame query implementations."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from benchbox.core.dataframe.query import DataFrameQuery, QueryCategory, QueryRegistry

if TYPE_CHECKING:
    from benchbox.core.dataframe.context import DataFrameContext


REGISTRY = QueryRegistry("tpcds_obt")


def q1_expression_impl(ctx: DataFrameContext) -> Any:
    table = ctx.get_table("tpcds_sales_returns_obt")
    return table.select(ctx.col("sale_id").count().alias("row_count"))


def q1_pandas_impl(ctx: DataFrameContext) -> Any:
    import pandas as pd

    table = ctx.get_table("tpcds_sales_returns_obt")
    return pd.DataFrame({"row_count": [len(table)]})


def q2_expression_impl(ctx: DataFrameContext) -> Any:
    table = ctx.get_table("tpcds_sales_returns_obt")
    return table.group_by("channel").agg(ctx.col("sale_id").count().alias("sales_count")).sort("channel")


def q2_pandas_impl(ctx: DataFrameContext) -> Any:
    table = ctx.get_table("tpcds_sales_returns_obt")
    result = table.groupby("channel", as_index=False).agg({"sale_id": "count"})
    return result.rename(columns={"sale_id": "sales_count"}).sort_values("channel")


def q3_expression_impl(ctx: DataFrameContext) -> Any:
    table = ctx.get_table("tpcds_sales_returns_obt")
    return table.filter(ctx.col("has_return") == ctx.lit("Y")).select(
        ctx.col("sale_id").count().alias("returned_sales"),
        ctx.col("return_amount").sum().alias("total_return_amount"),
    )


def q3_pandas_impl(ctx: DataFrameContext) -> Any:
    import pandas as pd

    table = ctx.get_table("tpcds_sales_returns_obt")
    filtered = table[table["has_return"] == "Y"]
    return pd.DataFrame(
        {
            "returned_sales": [len(filtered)],
            "total_return_amount": [float(filtered["return_amount"].sum()) if len(filtered) else 0.0],
        }
    )


def _register_queries() -> None:
    REGISTRY.register(
        DataFrameQuery(
            query_id="Q1",
            query_name="obt_row_count",
            description="Total row count for OBT table",
            categories=[QueryCategory.AGGREGATE],
            expression_impl=q1_expression_impl,
            pandas_impl=q1_pandas_impl,
            sql_equivalent="SELECT COUNT(*) AS row_count FROM tpcds_sales_returns_obt",
        )
    )
    REGISTRY.register(
        DataFrameQuery(
            query_id="Q2",
            query_name="obt_channel_distribution",
            description="Count sales rows by channel",
            categories=[QueryCategory.AGGREGATE, QueryCategory.GROUP_BY],
            expression_impl=q2_expression_impl,
            pandas_impl=q2_pandas_impl,
            sql_equivalent=(
                "SELECT channel, COUNT(sale_id) AS sales_count "
                "FROM tpcds_sales_returns_obt GROUP BY channel ORDER BY channel"
            ),
        )
    )
    REGISTRY.register(
        DataFrameQuery(
            query_id="Q3",
            query_name="obt_returns_summary",
            description="Summarize returned sales and amount",
            categories=[QueryCategory.AGGREGATE, QueryCategory.FILTER],
            expression_impl=q3_expression_impl,
            pandas_impl=q3_pandas_impl,
            sql_equivalent=(
                "SELECT COUNT(sale_id) AS returned_sales, SUM(return_amount) AS total_return_amount "
                "FROM tpcds_sales_returns_obt WHERE has_return = 'Y'"
            ),
        )
    )


_register_queries()


def get_dataframe_queries() -> list[DataFrameQuery]:
    """Return all registered OBT DataFrame queries in deterministic order."""
    return REGISTRY.get_all_queries()
