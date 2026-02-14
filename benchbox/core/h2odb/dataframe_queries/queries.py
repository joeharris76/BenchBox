"""H2ODB DataFrame query implementations.

All 10 H2ODB benchmark queries implemented for both Expression and Pandas families.
Single table (trips), no joins, no parameters.

Queries:
- Q1: Basic count
- Q2: Sum and mean of fare_amount
- Q3: Sum by passenger_count
- Q4: Sum and mean by passenger_count
- Q5: Sum by passenger_count and vendor_id
- Q6: Sum and mean by passenger_count and vendor_id
- Q7: Sum by hour of pickup_datetime
- Q8: Sum by year and hour of pickup_datetime
- Q9: Percentiles by passenger_count
- Q10: Top 10 pickup locations by trip count

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

from __future__ import annotations

from typing import Any

from benchbox.core.dataframe.context import DataFrameContext
from benchbox.core.dataframe.query import DataFrameQuery, QueryCategory

from .registry import register_query

# =============================================================================
# Q1: Basic count
# =============================================================================


def q1_expression_impl(ctx: DataFrameContext) -> Any:
    """Q1: COUNT(*)."""
    trips = ctx.get_table("trips")
    col = ctx.col
    return trips.select(col("vendor_id").count().alias("count"))


def q1_pandas_impl(ctx: DataFrameContext) -> Any:
    """Q1: COUNT(*)."""
    import pandas as pd

    trips = ctx.get_table("trips")
    return pd.DataFrame({"count": [len(trips)]})


# =============================================================================
# Q2: Sum and mean of fare_amount
# =============================================================================


def q2_expression_impl(ctx: DataFrameContext) -> Any:
    """Q2: SUM and AVG of fare_amount."""
    trips = ctx.get_table("trips")
    col = ctx.col
    return trips.select(
        col("fare_amount").sum().alias("sum_fare_amount"),
        col("fare_amount").mean().alias("mean_fare_amount"),
    )


def q2_pandas_impl(ctx: DataFrameContext) -> Any:
    """Q2: SUM and AVG of fare_amount."""
    import pandas as pd

    trips = ctx.get_table("trips")
    return pd.DataFrame(
        {
            "sum_fare_amount": [trips["fare_amount"].sum()],
            "mean_fare_amount": [trips["fare_amount"].mean()],
        }
    )


# =============================================================================
# Q3: Sum by passenger_count
# =============================================================================


def q3_expression_impl(ctx: DataFrameContext) -> Any:
    """Q3: SUM(fare_amount) GROUP BY passenger_count."""
    trips = ctx.get_table("trips")
    col = ctx.col
    return (
        trips.group_by("passenger_count").agg(col("fare_amount").sum().alias("sum_fare_amount")).sort("passenger_count")
    )


def q3_pandas_impl(ctx: DataFrameContext) -> Any:
    """Q3: SUM(fare_amount) GROUP BY passenger_count."""
    trips = ctx.get_table("trips")
    return (
        trips.groupby(["passenger_count"], as_index=False)
        .agg(sum_fare_amount=("fare_amount", "sum"))
        .sort_values("passenger_count")
    )


# =============================================================================
# Q4: Sum and mean by passenger_count
# =============================================================================


def q4_expression_impl(ctx: DataFrameContext) -> Any:
    """Q4: SUM and AVG of fare_amount GROUP BY passenger_count."""
    trips = ctx.get_table("trips")
    col = ctx.col
    return (
        trips.group_by("passenger_count")
        .agg(
            col("fare_amount").sum().alias("sum_fare_amount"),
            col("fare_amount").mean().alias("mean_fare_amount"),
        )
        .sort("passenger_count")
    )


def q4_pandas_impl(ctx: DataFrameContext) -> Any:
    """Q4: SUM and AVG of fare_amount GROUP BY passenger_count."""
    trips = ctx.get_table("trips")
    return (
        trips.groupby(["passenger_count"], as_index=False)
        .agg(sum_fare_amount=("fare_amount", "sum"), mean_fare_amount=("fare_amount", "mean"))
        .sort_values("passenger_count")
    )


# =============================================================================
# Q5: Sum by passenger_count and vendor_id
# =============================================================================


def q5_expression_impl(ctx: DataFrameContext) -> Any:
    """Q5: SUM(fare_amount) GROUP BY passenger_count, vendor_id."""
    trips = ctx.get_table("trips")
    col = ctx.col
    return (
        trips.group_by("passenger_count", "vendor_id")
        .agg(col("fare_amount").sum().alias("sum_fare_amount"))
        .sort(["passenger_count", "vendor_id"])
    )


def q5_pandas_impl(ctx: DataFrameContext) -> Any:
    """Q5: SUM(fare_amount) GROUP BY passenger_count, vendor_id."""
    trips = ctx.get_table("trips")
    return (
        trips.groupby(["passenger_count", "vendor_id"], as_index=False)
        .agg(sum_fare_amount=("fare_amount", "sum"))
        .sort_values(["passenger_count", "vendor_id"])
    )


# =============================================================================
# Q6: Sum and mean by passenger_count and vendor_id
# =============================================================================


def q6_expression_impl(ctx: DataFrameContext) -> Any:
    """Q6: SUM and AVG of fare_amount GROUP BY passenger_count, vendor_id."""
    trips = ctx.get_table("trips")
    col = ctx.col
    return (
        trips.group_by("passenger_count", "vendor_id")
        .agg(
            col("fare_amount").sum().alias("sum_fare_amount"),
            col("fare_amount").mean().alias("mean_fare_amount"),
        )
        .sort(["passenger_count", "vendor_id"])
    )


def q6_pandas_impl(ctx: DataFrameContext) -> Any:
    """Q6: SUM and AVG of fare_amount GROUP BY passenger_count, vendor_id."""
    trips = ctx.get_table("trips")
    return (
        trips.groupby(["passenger_count", "vendor_id"], as_index=False)
        .agg(sum_fare_amount=("fare_amount", "sum"), mean_fare_amount=("fare_amount", "mean"))
        .sort_values(["passenger_count", "vendor_id"])
    )


# =============================================================================
# Q7: Sum by hour of pickup_datetime
# =============================================================================


def q7_expression_impl(ctx: DataFrameContext) -> Any:
    """Q7: SUM(fare_amount) GROUP BY EXTRACT(HOUR)."""
    trips = ctx.get_table("trips")
    col = ctx.col
    return (
        trips.with_columns(col("pickup_datetime").dt.hour().alias("hour"))
        .group_by("hour")
        .agg(col("fare_amount").sum().alias("sum_fare_amount"))
        .sort("hour")
    )


def q7_pandas_impl(ctx: DataFrameContext) -> Any:
    """Q7: SUM(fare_amount) GROUP BY EXTRACT(HOUR)."""
    trips = ctx.get_table("trips")
    df = trips.copy()
    df["hour"] = df["pickup_datetime"].dt.hour
    return df.groupby(["hour"], as_index=False).agg(sum_fare_amount=("fare_amount", "sum")).sort_values("hour")


# =============================================================================
# Q8: Sum by year and hour of pickup_datetime
# =============================================================================


def q8_expression_impl(ctx: DataFrameContext) -> Any:
    """Q8: SUM(fare_amount) GROUP BY EXTRACT(YEAR), EXTRACT(HOUR)."""
    trips = ctx.get_table("trips")
    col = ctx.col
    return (
        trips.with_columns(
            col("pickup_datetime").dt.year().alias("year"),
            col("pickup_datetime").dt.hour().alias("hour"),
        )
        .group_by("year", "hour")
        .agg(col("fare_amount").sum().alias("sum_fare_amount"))
        .sort(["year", "hour"])
    )


def q8_pandas_impl(ctx: DataFrameContext) -> Any:
    """Q8: SUM(fare_amount) GROUP BY EXTRACT(YEAR), EXTRACT(HOUR)."""
    trips = ctx.get_table("trips")
    df = trips.copy()
    df["year"] = df["pickup_datetime"].dt.year
    df["hour"] = df["pickup_datetime"].dt.hour
    return (
        df.groupby(["year", "hour"], as_index=False)
        .agg(sum_fare_amount=("fare_amount", "sum"))
        .sort_values(["year", "hour"])
    )


# =============================================================================
# Q9: Percentiles by passenger_count
# =============================================================================


def q9_expression_impl(ctx: DataFrameContext) -> Any:
    """Q9: PERCENTILE_CONT(0.5, 0.9) of fare_amount GROUP BY passenger_count."""
    trips = ctx.get_table("trips")
    col = ctx.col
    return (
        trips.group_by("passenger_count")
        .agg(
            col("fare_amount").quantile(0.5).alias("median_fare_amount"),
            col("fare_amount").quantile(0.9).alias("p90_fare_amount"),
        )
        .sort("passenger_count")
    )


def q9_pandas_impl(ctx: DataFrameContext) -> Any:
    """Q9: PERCENTILE_CONT(0.5, 0.9) of fare_amount GROUP BY passenger_count."""
    trips = ctx.get_table("trips")
    grouped = trips.groupby("passenger_count")["fare_amount"]
    median = grouped.quantile(0.5).reset_index().rename(columns={"fare_amount": "median_fare_amount"})
    p90 = grouped.quantile(0.9).reset_index().rename(columns={"fare_amount": "p90_fare_amount"})
    return median.merge(p90, on="passenger_count").sort_values("passenger_count")


# =============================================================================
# Q10: Top 10 pickup locations by trip count
# =============================================================================


def q10_expression_impl(ctx: DataFrameContext) -> Any:
    """Q10: Top 10 pickup locations by trip count."""
    trips = ctx.get_table("trips")
    col = ctx.col
    return (
        trips.filter(col("pickup_location_id").is_not_null())
        .group_by("pickup_location_id")
        .agg(col("pickup_location_id").count().alias("trip_count"))
        .sort("trip_count", descending=True)
        .limit(10)
    )


def q10_pandas_impl(ctx: DataFrameContext) -> Any:
    """Q10: Top 10 pickup locations by trip count."""
    trips = ctx.get_table("trips")
    filtered = trips[trips["pickup_location_id"].notna()]
    return (
        filtered.groupby(["pickup_location_id"], as_index=False)
        .agg(trip_count=("pickup_location_id", "count"))
        .sort_values("trip_count", ascending=False)
        .head(10)
    )


# =============================================================================
# Query Registration
# =============================================================================


def _register_all_queries() -> None:
    """Register all 10 H2ODB DataFrame queries."""
    register_query(
        DataFrameQuery(
            query_id="Q1",
            query_name="Count",
            description="Basic COUNT(*) full table scan",
            categories=[QueryCategory.SCAN, QueryCategory.AGGREGATE],
            expression_impl=q1_expression_impl,
            pandas_impl=q1_pandas_impl,
        )
    )
    register_query(
        DataFrameQuery(
            query_id="Q2",
            query_name="Sum and Mean",
            description="SUM and AVG of fare_amount (scalar aggregation)",
            categories=[QueryCategory.AGGREGATE],
            expression_impl=q2_expression_impl,
            pandas_impl=q2_pandas_impl,
        )
    )
    register_query(
        DataFrameQuery(
            query_id="Q3",
            query_name="Sum by Passenger Count",
            description="SUM(fare_amount) GROUP BY passenger_count",
            categories=[QueryCategory.GROUP_BY, QueryCategory.AGGREGATE],
            expression_impl=q3_expression_impl,
            pandas_impl=q3_pandas_impl,
        )
    )
    register_query(
        DataFrameQuery(
            query_id="Q4",
            query_name="Sum and Mean by Passenger Count",
            description="SUM and AVG of fare_amount GROUP BY passenger_count",
            categories=[QueryCategory.GROUP_BY, QueryCategory.AGGREGATE],
            expression_impl=q4_expression_impl,
            pandas_impl=q4_pandas_impl,
        )
    )
    register_query(
        DataFrameQuery(
            query_id="Q5",
            query_name="Sum by Passenger and Vendor",
            description="SUM(fare_amount) GROUP BY passenger_count, vendor_id",
            categories=[QueryCategory.GROUP_BY, QueryCategory.AGGREGATE],
            expression_impl=q5_expression_impl,
            pandas_impl=q5_pandas_impl,
        )
    )
    register_query(
        DataFrameQuery(
            query_id="Q6",
            query_name="Sum and Mean by Passenger and Vendor",
            description="SUM and AVG of fare_amount GROUP BY passenger_count, vendor_id",
            categories=[QueryCategory.GROUP_BY, QueryCategory.AGGREGATE],
            expression_impl=q6_expression_impl,
            pandas_impl=q6_pandas_impl,
        )
    )
    register_query(
        DataFrameQuery(
            query_id="Q7",
            query_name="Sum by Hour",
            description="SUM(fare_amount) GROUP BY EXTRACT(HOUR FROM pickup_datetime)",
            categories=[QueryCategory.GROUP_BY, QueryCategory.AGGREGATE, QueryCategory.ANALYTICAL],
            expression_impl=q7_expression_impl,
            pandas_impl=q7_pandas_impl,
        )
    )
    register_query(
        DataFrameQuery(
            query_id="Q8",
            query_name="Sum by Year and Hour",
            description="SUM(fare_amount) GROUP BY EXTRACT(YEAR), EXTRACT(HOUR)",
            categories=[QueryCategory.GROUP_BY, QueryCategory.AGGREGATE, QueryCategory.ANALYTICAL],
            expression_impl=q8_expression_impl,
            pandas_impl=q8_pandas_impl,
        )
    )
    register_query(
        DataFrameQuery(
            query_id="Q9",
            query_name="Percentiles by Passenger Count",
            description="PERCENTILE_CONT(0.5, 0.9) of fare_amount GROUP BY passenger_count",
            categories=[QueryCategory.GROUP_BY, QueryCategory.AGGREGATE, QueryCategory.ANALYTICAL],
            expression_impl=q9_expression_impl,
            pandas_impl=q9_pandas_impl,
        )
    )
    register_query(
        DataFrameQuery(
            query_id="Q10",
            query_name="Top Pickup Locations",
            description="Top 10 pickup locations by trip count with WHERE IS NOT NULL",
            categories=[QueryCategory.FILTER, QueryCategory.GROUP_BY, QueryCategory.SORT],
            expression_impl=q10_expression_impl,
            pandas_impl=q10_pandas_impl,
        )
    )


_register_all_queries()
