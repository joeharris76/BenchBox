"""Multi-channel union helper for TPC-DS DataFrame queries.

TPC-DS has three sales channels (store, catalog, web) with different column
naming conventions. Many queries require UNION ALL across channels with
column name standardization.

This module provides:
- Column mapping definitions for each channel
- Helper functions for creating unified multi-channel views
- Support for both sales and returns fact tables

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    pass

# =============================================================================
# Column Mappings
# =============================================================================

# Sales fact table column mappings: standard_name -> channel-specific column
SALES_COLUMN_MAPPINGS: dict[str, dict[str, str]] = {
    "store": {
        # Keys / Foreign Keys
        "sold_date_sk": "ss_sold_date_sk",
        "sold_time_sk": "ss_sold_time_sk",
        "item_sk": "ss_item_sk",
        "customer_sk": "ss_customer_sk",
        "cdemo_sk": "ss_cdemo_sk",
        "hdemo_sk": "ss_hdemo_sk",
        "addr_sk": "ss_addr_sk",
        "store_sk": "ss_store_sk",
        "promo_sk": "ss_promo_sk",
        "ticket_number": "ss_ticket_number",
        # Measures
        "quantity": "ss_quantity",
        "wholesale_cost": "ss_wholesale_cost",
        "list_price": "ss_list_price",
        "sales_price": "ss_sales_price",
        "ext_discount_amt": "ss_ext_discount_amt",
        "ext_sales_price": "ss_ext_sales_price",
        "ext_wholesale_cost": "ss_ext_wholesale_cost",
        "ext_list_price": "ss_ext_list_price",
        "ext_tax": "ss_ext_tax",
        "coupon_amt": "ss_coupon_amt",
        "net_paid": "ss_net_paid",
        "net_paid_inc_tax": "ss_net_paid_inc_tax",
        "net_profit": "ss_net_profit",
    },
    "catalog": {
        # Keys / Foreign Keys
        "sold_date_sk": "cs_sold_date_sk",
        "sold_time_sk": "cs_sold_time_sk",
        "ship_date_sk": "cs_ship_date_sk",
        "item_sk": "cs_item_sk",
        "customer_sk": "cs_bill_customer_sk",
        "cdemo_sk": "cs_bill_cdemo_sk",
        "hdemo_sk": "cs_bill_hdemo_sk",
        "addr_sk": "cs_bill_addr_sk",
        "call_center_sk": "cs_call_center_sk",
        "catalog_page_sk": "cs_catalog_page_sk",
        "ship_mode_sk": "cs_ship_mode_sk",
        "warehouse_sk": "cs_warehouse_sk",
        "promo_sk": "cs_promo_sk",
        "order_number": "cs_order_number",
        # Measures
        "quantity": "cs_quantity",
        "wholesale_cost": "cs_wholesale_cost",
        "list_price": "cs_list_price",
        "sales_price": "cs_sales_price",
        "ext_discount_amt": "cs_ext_discount_amt",
        "ext_sales_price": "cs_ext_sales_price",
        "ext_wholesale_cost": "cs_ext_wholesale_cost",
        "ext_list_price": "cs_ext_list_price",
        "ext_ship_cost": "cs_ext_ship_cost",
        "ext_tax": "cs_ext_tax",
        "coupon_amt": "cs_coupon_amt",
        "net_paid": "cs_net_paid",
        "net_paid_inc_tax": "cs_net_paid_inc_tax",
        "net_paid_inc_ship": "cs_net_paid_inc_ship",
        "net_paid_inc_ship_tax": "cs_net_paid_inc_ship_tax",
        "net_profit": "cs_net_profit",
    },
    "web": {
        # Keys / Foreign Keys
        "sold_date_sk": "ws_sold_date_sk",
        "sold_time_sk": "ws_sold_time_sk",
        "ship_date_sk": "ws_ship_date_sk",
        "item_sk": "ws_item_sk",
        "customer_sk": "ws_bill_customer_sk",
        "cdemo_sk": "ws_bill_cdemo_sk",
        "hdemo_sk": "ws_bill_hdemo_sk",
        "addr_sk": "ws_bill_addr_sk",
        "web_page_sk": "ws_web_page_sk",
        "web_site_sk": "ws_web_site_sk",
        "ship_mode_sk": "ws_ship_mode_sk",
        "warehouse_sk": "ws_warehouse_sk",
        "promo_sk": "ws_promo_sk",
        "order_number": "ws_order_number",
        # Measures
        "quantity": "ws_quantity",
        "wholesale_cost": "ws_wholesale_cost",
        "list_price": "ws_list_price",
        "sales_price": "ws_sales_price",
        "ext_discount_amt": "ws_ext_discount_amt",
        "ext_sales_price": "ws_ext_sales_price",
        "ext_wholesale_cost": "ws_ext_wholesale_cost",
        "ext_list_price": "ws_ext_list_price",
        "ext_ship_cost": "ws_ext_ship_cost",
        "ext_tax": "ws_ext_tax",
        "coupon_amt": "ws_coupon_amt",
        "net_paid": "ws_net_paid",
        "net_paid_inc_tax": "ws_net_paid_inc_tax",
        "net_paid_inc_ship": "ws_net_paid_inc_ship",
        "net_paid_inc_ship_tax": "ws_net_paid_inc_ship_tax",
        "net_profit": "ws_net_profit",
    },
}

# Returns fact table column mappings
RETURNS_COLUMN_MAPPINGS: dict[str, dict[str, str]] = {
    "store": {
        # Keys
        "returned_date_sk": "sr_returned_date_sk",
        "return_time_sk": "sr_return_time_sk",
        "item_sk": "sr_item_sk",
        "customer_sk": "sr_customer_sk",
        "cdemo_sk": "sr_cdemo_sk",
        "hdemo_sk": "sr_hdemo_sk",
        "addr_sk": "sr_addr_sk",
        "store_sk": "sr_store_sk",
        "reason_sk": "sr_reason_sk",
        "ticket_number": "sr_ticket_number",
        # Measures
        "return_quantity": "sr_return_quantity",
        "return_amt": "sr_return_amt",
        "return_tax": "sr_return_tax",
        "return_amt_inc_tax": "sr_return_amt_inc_tax",
        "fee": "sr_fee",
        "return_ship_cost": "sr_return_ship_cost",
        "refunded_cash": "sr_refunded_cash",
        "reversed_charge": "sr_reversed_charge",
        "store_credit": "sr_store_credit",
        "net_loss": "sr_net_loss",
    },
    "catalog": {
        # Keys
        "returned_date_sk": "cr_returned_date_sk",
        "return_time_sk": "cr_return_time_sk",
        "item_sk": "cr_item_sk",
        "refunded_customer_sk": "cr_refunded_customer_sk",
        "refunded_cdemo_sk": "cr_refunded_cdemo_sk",
        "refunded_hdemo_sk": "cr_refunded_hdemo_sk",
        "refunded_addr_sk": "cr_refunded_addr_sk",
        "returning_customer_sk": "cr_returning_customer_sk",
        "returning_cdemo_sk": "cr_returning_cdemo_sk",
        "returning_hdemo_sk": "cr_returning_hdemo_sk",
        "returning_addr_sk": "cr_returning_addr_sk",
        "call_center_sk": "cr_call_center_sk",
        "catalog_page_sk": "cr_catalog_page_sk",
        "ship_mode_sk": "cr_ship_mode_sk",
        "warehouse_sk": "cr_warehouse_sk",
        "reason_sk": "cr_reason_sk",
        "order_number": "cr_order_number",
        # Measures
        "return_quantity": "cr_return_quantity",
        "return_amount": "cr_return_amount",
        "return_tax": "cr_return_tax",
        "return_amt_inc_tax": "cr_return_amt_inc_tax",
        "fee": "cr_fee",
        "return_ship_cost": "cr_return_ship_cost",
        "refunded_cash": "cr_refunded_cash",
        "reversed_charge": "cr_reversed_charge",
        "store_credit": "cr_store_credit",
        "net_loss": "cr_net_loss",
    },
    "web": {
        # Keys
        "returned_date_sk": "wr_returned_date_sk",
        "return_time_sk": "wr_return_time_sk",
        "item_sk": "wr_item_sk",
        "refunded_customer_sk": "wr_refunded_customer_sk",
        "refunded_cdemo_sk": "wr_refunded_cdemo_sk",
        "refunded_hdemo_sk": "wr_refunded_hdemo_sk",
        "refunded_addr_sk": "wr_refunded_addr_sk",
        "returning_customer_sk": "wr_returning_customer_sk",
        "returning_cdemo_sk": "wr_returning_cdemo_sk",
        "returning_hdemo_sk": "wr_returning_hdemo_sk",
        "returning_addr_sk": "wr_returning_addr_sk",
        "web_page_sk": "wr_web_page_sk",
        "reason_sk": "wr_reason_sk",
        "order_number": "wr_order_number",
        # Measures
        "return_quantity": "wr_return_quantity",
        "return_amt": "wr_return_amt",
        "return_tax": "wr_return_tax",
        "return_amt_inc_tax": "wr_return_amt_inc_tax",
        "fee": "wr_fee",
        "return_ship_cost": "wr_return_ship_cost",
        "refunded_cash": "wr_refunded_cash",
        "reversed_charge": "wr_reversed_charge",
        "account_credit": "wr_account_credit",
        "net_loss": "wr_net_loss",
    },
}

# Table names for each channel
SALES_TABLE_NAMES = {
    "store": "store_sales",
    "catalog": "catalog_sales",
    "web": "web_sales",
}

RETURNS_TABLE_NAMES = {
    "store": "store_returns",
    "catalog": "catalog_returns",
    "web": "web_returns",
}


# =============================================================================
# Union Helper Functions
# =============================================================================


def union_sales_channels_expression(
    ctx: Any,
    channels: list[str],
    columns: list[str],
    add_channel_col: bool = True,
) -> Any:
    """Union sales fact tables from multiple channels (expression family).

    Creates a UNION ALL of sales data from specified channels with
    standardized column names.

    Args:
        ctx: DataFrameContext (expression family)
        channels: List of channels to include: "store", "catalog", "web"
        columns: List of standard column names to select
        add_channel_col: Whether to add a 'channel' column (default True)

    Returns:
        Combined LazyFrame with standardized columns

    Example:
        ```python
        # Union store and web sales with quantity and price
        sales = union_sales_channels_expression(
            ctx,
            channels=["store", "web"],
            columns=["sold_date_sk", "item_sk", "quantity", "ext_sales_price"]
        )
        ```
    """

    col = ctx.col
    lit = ctx.lit
    channel_dfs = []

    for channel in channels:
        if channel not in SALES_COLUMN_MAPPINGS:
            raise ValueError(f"Unknown channel: {channel}. Valid: store, catalog, web")

        table_name = SALES_TABLE_NAMES[channel]
        mapping = SALES_COLUMN_MAPPINGS[channel]

        # Get table
        df = ctx.get_table(table_name)

        # Build select expressions: rename channel-specific columns to standard names
        select_exprs = []
        for std_col in columns:
            if std_col not in mapping:
                raise ValueError(
                    f"Column '{std_col}' not available for channel '{channel}'. "
                    f"Available columns: {list(mapping.keys())}"
                )
            channel_col = mapping[std_col]
            select_exprs.append(col(channel_col).alias(std_col))

        # Add channel indicator if requested
        if add_channel_col:
            select_exprs.append(lit(channel).alias("channel"))

        # Select and append
        channel_df = df.select(select_exprs)
        channel_dfs.append(channel_df)

    # Union all channels
    return ctx.concat(channel_dfs)


def union_sales_channels_pandas(
    ctx: Any,
    channels: list[str],
    columns: list[str],
    add_channel_col: bool = True,
) -> Any:
    """Union sales fact tables from multiple channels (pandas family).

    Creates a UNION ALL of sales data from specified channels with
    standardized column names.

    Args:
        ctx: DataFrameContext (pandas family)
        channels: List of channels to include: "store", "catalog", "web"
        columns: List of standard column names to select
        add_channel_col: Whether to add a 'channel' column (default True)

    Returns:
        Combined DataFrame with standardized columns
    """
    channel_dfs = []

    for channel in channels:
        if channel not in SALES_COLUMN_MAPPINGS:
            raise ValueError(f"Unknown channel: {channel}. Valid: store, catalog, web")

        table_name = SALES_TABLE_NAMES[channel]
        mapping = SALES_COLUMN_MAPPINGS[channel]

        # Get table
        df = ctx.get_table(table_name)

        # Build column selection: channel-specific -> standard
        select_cols = {}
        for std_col in columns:
            if std_col not in mapping:
                raise ValueError(f"Column '{std_col}' not available for channel '{channel}'")
            channel_col = mapping[std_col]
            select_cols[channel_col] = std_col

        # Select and rename columns
        channel_df = df[list(select_cols.keys())].rename(columns=select_cols)

        # Add channel indicator if requested
        if add_channel_col:
            channel_df = channel_df.copy()
            channel_df["channel"] = channel

        channel_dfs.append(channel_df)

    # Union all channels using ctx.concat for Dask compatibility
    return ctx.concat(channel_dfs)


def union_returns_channels_expression(
    ctx: Any,
    channels: list[str],
    columns: list[str],
    add_channel_col: bool = True,
) -> Any:
    """Union returns fact tables from multiple channels (expression family).

    Similar to union_sales_channels but for returns tables.

    Args:
        ctx: DataFrameContext (expression family)
        channels: List of channels to include: "store", "catalog", "web"
        columns: List of standard column names to select
        add_channel_col: Whether to add a 'channel' column (default True)

    Returns:
        Combined LazyFrame with standardized columns
    """

    col = ctx.col
    lit = ctx.lit
    channel_dfs = []

    for channel in channels:
        if channel not in RETURNS_COLUMN_MAPPINGS:
            raise ValueError(f"Unknown channel: {channel}. Valid: store, catalog, web")

        table_name = RETURNS_TABLE_NAMES[channel]
        mapping = RETURNS_COLUMN_MAPPINGS[channel]

        # Get table
        df = ctx.get_table(table_name)

        # Build select expressions
        select_exprs = []
        for std_col in columns:
            if std_col not in mapping:
                raise ValueError(f"Column '{std_col}' not available for channel '{channel}'")
            channel_col = mapping[std_col]
            select_exprs.append(col(channel_col).alias(std_col))

        if add_channel_col:
            select_exprs.append(lit(channel).alias("channel"))

        channel_df = df.select(select_exprs)
        channel_dfs.append(channel_df)

    return ctx.concat(channel_dfs)


def union_returns_channels_pandas(
    ctx: Any,
    channels: list[str],
    columns: list[str],
    add_channel_col: bool = True,
) -> Any:
    """Union returns fact tables from multiple channels (pandas family)."""
    channel_dfs = []

    for channel in channels:
        if channel not in RETURNS_COLUMN_MAPPINGS:
            raise ValueError(f"Unknown channel: {channel}. Valid: store, catalog, web")

        table_name = RETURNS_TABLE_NAMES[channel]
        mapping = RETURNS_COLUMN_MAPPINGS[channel]

        df = ctx.get_table(table_name)

        select_cols = {}
        for std_col in columns:
            if std_col not in mapping:
                raise ValueError(f"Column '{std_col}' not available for channel '{channel}'")
            channel_col = mapping[std_col]
            select_cols[channel_col] = std_col

        channel_df = df[list(select_cols.keys())].rename(columns=select_cols)

        if add_channel_col:
            channel_df = channel_df.copy()
            channel_df["channel"] = channel

        channel_dfs.append(channel_df)

    # Union all channels using ctx.concat for Dask compatibility
    return ctx.concat(channel_dfs)


# =============================================================================
# Convenience Functions
# =============================================================================


def get_sales_column(channel: str, standard_name: str) -> str:
    """Get the channel-specific column name for a standard column.

    Args:
        channel: Channel name ("store", "catalog", "web")
        standard_name: Standard column name

    Returns:
        Channel-specific column name
    """
    if channel not in SALES_COLUMN_MAPPINGS:
        raise ValueError(f"Unknown channel: {channel}")
    if standard_name not in SALES_COLUMN_MAPPINGS[channel]:
        raise ValueError(f"Unknown column: {standard_name} for channel {channel}")
    return SALES_COLUMN_MAPPINGS[channel][standard_name]


def get_returns_column(channel: str, standard_name: str) -> str:
    """Get the channel-specific returns column name."""
    if channel not in RETURNS_COLUMN_MAPPINGS:
        raise ValueError(f"Unknown channel: {channel}")
    if standard_name not in RETURNS_COLUMN_MAPPINGS[channel]:
        raise ValueError(f"Unknown column: {standard_name} for channel {channel}")
    return RETURNS_COLUMN_MAPPINGS[channel][standard_name]


def get_available_sales_columns(channel: str | None = None) -> list[str]:
    """Get list of available standard column names for sales tables.

    Args:
        channel: Optional specific channel, or None for all channels

    Returns:
        List of standard column names
    """
    if channel:
        return list(SALES_COLUMN_MAPPINGS.get(channel, {}).keys())

    # Find columns available in all channels
    all_cols = set(SALES_COLUMN_MAPPINGS["store"].keys())
    all_cols &= set(SALES_COLUMN_MAPPINGS["catalog"].keys())
    all_cols &= set(SALES_COLUMN_MAPPINGS["web"].keys())
    return sorted(all_cols)


def get_available_returns_columns(channel: str | None = None) -> list[str]:
    """Get list of available standard column names for returns tables."""
    if channel:
        return list(RETURNS_COLUMN_MAPPINGS.get(channel, {}).keys())

    all_cols = set(RETURNS_COLUMN_MAPPINGS["store"].keys())
    all_cols &= set(RETURNS_COLUMN_MAPPINGS["catalog"].keys())
    all_cols &= set(RETURNS_COLUMN_MAPPINGS["web"].keys())
    return sorted(all_cols)
