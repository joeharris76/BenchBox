"""Schema definition for the TPC-DS One Big Table benchmark.

This module builds a single wide table (`tpcds_sales_returns_obt`) that merges
all TPC-DS sales facts, returns facts, and relevant dimension attributes into
one relation. Column definitions carry lineage metadata so the ETL step can
project channel-specific columns into a canonical layout while keeping a single
table as the only benchmark artifact.
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Iterable
from dataclasses import dataclass
from functools import cache

from benchbox.core.tpcds.schema.models import DataType, Table
from benchbox.core.tpcds.schema.tables import (
    CALL_CENTER,
    CATALOG_PAGE,
    CUSTOMER,
    CUSTOMER_ADDRESS,
    CUSTOMER_DEMOGRAPHICS,
    DATE_DIM,
    HOUSEHOLD_DEMOGRAPHICS,
    ITEM,
    PROMOTION,
    REASON,
    SHIP_MODE,
    STORE,
    TIME_DIM,
    WAREHOUSE,
    WEB_PAGE,
    WEB_SITE,
)

OBT_TABLE_NAME = "tpcds_sales_returns_obt"
DEFAULT_MODE = "full"
ALLOWED_MODES = {DEFAULT_MODE, "minimal"}


@dataclass(frozen=True)
class OBTColumn:
    """Column definition with lineage metadata for the OBT table."""

    name: str
    data_type: DataType
    size: int | None = None
    nullable: bool = True
    primary_key: bool = False
    source_table: str | None = None
    source_column: str | None = None
    role: str | None = None
    description: str | None = None

    def sql_type(self) -> str:
        """Return the SQL type string for this column."""
        if self.data_type in (DataType.VARCHAR, DataType.CHAR) and self.size is not None:
            return f"{self.data_type.value}({self.size})"
        return self.data_type.value


@dataclass(frozen=True)
class DimensionRole:
    """Represents a dimension role to inline into the OBT."""

    name: str
    table: Table
    prefix: str
    description: str | None = None


@dataclass(frozen=True)
class OBTTable:
    """Represents the single OBT table and generates DDL."""

    name: str
    columns: tuple[OBTColumn, ...]

    def get_primary_key(self) -> list[str]:
        """Return primary key column names if defined."""
        return [col.name for col in self.columns if col.primary_key]

    def get_create_table_sql(self) -> str:
        """Generate CREATE TABLE DDL for the OBT."""
        column_defs: list[str] = []
        pk_columns = self.get_primary_key()

        for col in self.columns:
            col_def = f"{col.name} {col.sql_type()}"
            if not col.nullable:
                col_def += " NOT NULL"
            column_defs.append(col_def)

        if pk_columns:
            column_defs.append(f"PRIMARY KEY ({', '.join(pk_columns)})")

        sql = f"CREATE TABLE {self.name} (\n    "
        sql += ",\n    ".join(column_defs)
        sql += "\n);"
        return sql


def _fact_column(
    name: str,
    data_type: DataType,
    *,
    size: int | None = None,
    nullable: bool = True,
    source_table: str | None = None,
    source_column: str | None = None,
    description: str | None = None,
) -> OBTColumn:
    """Create a fact-level OBT column with consistent metadata."""
    return OBTColumn(
        name=name,
        data_type=data_type,
        size=size,
        nullable=nullable,
        source_table=source_table,
        source_column=source_column,
        role="fact",
        description=description,
    )


CORE_FACT_COLUMNS: tuple[OBTColumn, ...] = (
    _fact_column(
        "channel",
        DataType.VARCHAR,
        size=10,
        nullable=False,
        description="Channel discriminator: store|web|catalog",
    ),
    _fact_column(
        "sale_id",
        DataType.INTEGER,
        nullable=False,
        source_table="store_sales|web_sales|catalog_sales",
        source_column="ss_ticket_number|ws_order_number|cs_order_number",
        description="Channel-specific sale identifier (ticket/order number)",
    ),
    _fact_column(
        "item_sk",
        DataType.INTEGER,
        nullable=False,
        source_table="store_sales|web_sales|catalog_sales",
        source_column="ss_item_sk|ws_item_sk|cs_item_sk",
    ),
    _fact_column(
        "sold_date_sk",
        DataType.INTEGER,
        source_table="store_sales|web_sales|catalog_sales",
        source_column="ss_sold_date_sk|ws_sold_date_sk|cs_sold_date_sk",
    ),
    _fact_column(
        "sold_time_sk",
        DataType.INTEGER,
        source_table="store_sales|web_sales|catalog_sales",
        source_column="ss_sold_time_sk|ws_sold_time_sk|cs_sold_time_sk",
    ),
    _fact_column(
        "ship_date_sk",
        DataType.INTEGER,
        source_table="web_sales|catalog_sales",
        source_column="ws_ship_date_sk|cs_ship_date_sk",
    ),
    _fact_column(
        "returned_date_sk",
        DataType.INTEGER,
        source_table="store_returns|web_returns|catalog_returns",
        source_column="sr_returned_date_sk|wr_returned_date_sk|cr_returned_date_sk",
    ),
    _fact_column(
        "returned_time_sk",
        DataType.INTEGER,
        source_table="store_returns|web_returns|catalog_returns",
        source_column="sr_return_time_sk|wr_returned_time_sk|cr_returned_time_sk",
    ),
    _fact_column(
        "bill_customer_sk",
        DataType.INTEGER,
        source_table="store_sales|web_sales|catalog_sales",
        source_column="ss_customer_sk|ws_bill_customer_sk|cs_bill_customer_sk",
    ),
    _fact_column(
        "bill_cdemo_sk",
        DataType.INTEGER,
        source_table="store_sales|web_sales|catalog_sales",
        source_column="ss_cdemo_sk|ws_bill_cdemo_sk|cs_bill_cdemo_sk",
    ),
    _fact_column(
        "bill_hdemo_sk",
        DataType.INTEGER,
        source_table="store_sales|web_sales|catalog_sales",
        source_column="ss_hdemo_sk|ws_bill_hdemo_sk|cs_bill_hdemo_sk",
    ),
    _fact_column(
        "bill_addr_sk",
        DataType.INTEGER,
        source_table="store_sales|web_sales|catalog_sales",
        source_column="ss_addr_sk|ws_bill_addr_sk|cs_bill_addr_sk",
    ),
    _fact_column(
        "ship_customer_sk",
        DataType.INTEGER,
        source_table="store_sales|web_sales|catalog_sales",
        source_column="ss_customer_sk|ws_ship_customer_sk|cs_ship_customer_sk",
    ),
    _fact_column(
        "ship_cdemo_sk",
        DataType.INTEGER,
        source_table="store_sales|web_sales|catalog_sales",
        source_column="ss_cdemo_sk|ws_ship_cdemo_sk|cs_ship_cdemo_sk",
    ),
    _fact_column(
        "ship_hdemo_sk",
        DataType.INTEGER,
        source_table="store_sales|web_sales|catalog_sales",
        source_column="ss_hdemo_sk|ws_ship_hdemo_sk|cs_ship_hdemo_sk",
    ),
    _fact_column(
        "ship_addr_sk",
        DataType.INTEGER,
        source_table="store_sales|web_sales|catalog_sales",
        source_column="ss_addr_sk|ws_ship_addr_sk|cs_ship_addr_sk",
    ),
    _fact_column(
        "store_sk",
        DataType.INTEGER,
        source_table="store_sales|store_returns",
        source_column="ss_store_sk|sr_store_sk",
    ),
    _fact_column(
        "web_site_sk",
        DataType.INTEGER,
        source_table="web_sales|web_returns",
        source_column="ws_web_site_sk|wr_web_site_sk",
    ),
    _fact_column(
        "web_page_sk",
        DataType.INTEGER,
        source_table="web_sales|web_returns",
        source_column="ws_web_page_sk|wr_web_page_sk",
    ),
    _fact_column(
        "call_center_sk",
        DataType.INTEGER,
        source_table="catalog_sales|catalog_returns",
        source_column="cs_call_center_sk|cr_call_center_sk",
    ),
    _fact_column(
        "catalog_page_sk",
        DataType.INTEGER,
        source_table="catalog_sales|catalog_returns",
        source_column="cs_catalog_page_sk|cr_catalog_page_sk",
    ),
    _fact_column(
        "ship_mode_sk",
        DataType.INTEGER,
        source_table="web_sales|catalog_sales|web_returns|catalog_returns",
        source_column="ws_ship_mode_sk|cs_ship_mode_sk|wr_ship_mode_sk|cr_ship_mode_sk",
    ),
    _fact_column(
        "warehouse_sk",
        DataType.INTEGER,
        source_table="web_sales|catalog_sales|web_returns|catalog_returns",
        source_column="ws_warehouse_sk|cs_warehouse_sk|wr_warehouse_sk|cr_warehouse_sk",
    ),
    _fact_column(
        "promo_sk",
        DataType.INTEGER,
        source_table="store_sales|web_sales|catalog_sales",
        source_column="ss_promo_sk|ws_promo_sk|cs_promo_sk",
    ),
    _fact_column(
        "reason_sk",
        DataType.INTEGER,
        source_table="store_returns|web_returns|catalog_returns",
        source_column="sr_reason_sk|wr_reason_sk|cr_reason_sk",
    ),
    _fact_column(
        "quantity",
        DataType.INTEGER,
        source_table="store_sales|web_sales|catalog_sales",
        source_column="ss_quantity|ws_quantity|cs_quantity",
    ),
    _fact_column(
        "wholesale_cost",
        DataType.DECIMAL,
        source_table="store_sales|web_sales|catalog_sales",
        source_column="ss_wholesale_cost|ws_wholesale_cost|cs_wholesale_cost",
    ),
    _fact_column(
        "list_price",
        DataType.DECIMAL,
        source_table="store_sales|web_sales|catalog_sales",
        source_column="ss_list_price|ws_list_price|cs_list_price",
    ),
    _fact_column(
        "sales_price",
        DataType.DECIMAL,
        source_table="store_sales|web_sales|catalog_sales",
        source_column="ss_sales_price|ws_sales_price|cs_sales_price",
    ),
    _fact_column(
        "ext_discount_amt",
        DataType.DECIMAL,
        source_table="store_sales|web_sales|catalog_sales",
        source_column="ss_ext_discount_amt|ws_ext_discount_amt|cs_ext_discount_amt",
    ),
    _fact_column(
        "ext_sales_price",
        DataType.DECIMAL,
        source_table="store_sales|web_sales|catalog_sales",
        source_column="ss_ext_sales_price|ws_ext_sales_price|cs_ext_sales_price",
    ),
    _fact_column(
        "ext_wholesale_cost",
        DataType.DECIMAL,
        source_table="store_sales|web_sales|catalog_sales",
        source_column="ss_ext_wholesale_cost|ws_ext_wholesale_cost|cs_ext_wholesale_cost",
    ),
    _fact_column(
        "ext_list_price",
        DataType.DECIMAL,
        source_table="store_sales|web_sales|catalog_sales",
        source_column="ss_ext_list_price|ws_ext_list_price|cs_ext_list_price",
    ),
    _fact_column(
        "ext_tax",
        DataType.DECIMAL,
        source_table="store_sales|web_sales|catalog_sales",
        source_column="ss_ext_tax|ws_ext_tax|cs_ext_tax",
    ),
    _fact_column(
        "coupon_amt",
        DataType.DECIMAL,
        source_table="store_sales|web_sales|catalog_sales",
        source_column="ss_coupon_amt|ws_coupon_amt|cs_coupon_amt",
    ),
    _fact_column(
        "ext_ship_cost",
        DataType.DECIMAL,
        source_table="web_sales|catalog_sales",
        source_column="ws_ext_ship_cost|cs_ext_ship_cost",
    ),
    _fact_column(
        "net_paid",
        DataType.DECIMAL,
        source_table="store_sales|web_sales|catalog_sales",
        source_column="ss_net_paid|ws_net_paid|cs_net_paid",
    ),
    _fact_column(
        "net_paid_inc_tax",
        DataType.DECIMAL,
        source_table="store_sales|web_sales|catalog_sales",
        source_column="ss_net_paid_inc_tax|ws_net_paid_inc_tax|cs_net_paid_inc_tax",
    ),
    _fact_column(
        "net_paid_inc_ship",
        DataType.DECIMAL,
        source_table="web_sales|catalog_sales",
        source_column="ws_net_paid_inc_ship|cs_net_paid_inc_ship",
    ),
    _fact_column(
        "net_paid_inc_ship_tax",
        DataType.DECIMAL,
        source_table="web_sales|catalog_sales",
        source_column="ws_net_paid_inc_ship_tax|cs_net_paid_inc_ship_tax",
    ),
    _fact_column(
        "net_profit",
        DataType.DECIMAL,
        source_table="store_sales|web_sales|catalog_sales",
        source_column="ss_net_profit|ws_net_profit|cs_net_profit",
    ),
    _fact_column(
        "return_quantity",
        DataType.INTEGER,
        source_table="store_returns|web_returns|catalog_returns",
        source_column="sr_return_quantity|wr_return_quantity|cr_return_quantity",
    ),
    _fact_column(
        "return_amount",
        DataType.DECIMAL,
        source_table="store_returns|web_returns|catalog_returns",
        source_column="sr_return_amt|wr_return_amt|cr_return_amount",
    ),
    _fact_column(
        "return_tax",
        DataType.DECIMAL,
        source_table="store_returns|web_returns|catalog_returns",
        source_column="sr_return_tax|wr_return_tax|cr_return_tax",
    ),
    _fact_column(
        "return_amount_inc_tax",
        DataType.DECIMAL,
        source_table="store_returns|web_returns|catalog_returns",
        source_column="sr_return_amt_inc_tax|wr_return_amt_inc_tax|cr_return_amt_inc_tax",
    ),
    _fact_column(
        "return_fee",
        DataType.DECIMAL,
        source_table="store_returns|web_returns|catalog_returns",
        source_column="sr_fee|wr_fee|cr_fee",
    ),
    _fact_column(
        "return_ship_cost",
        DataType.DECIMAL,
        source_table="store_returns|web_returns|catalog_returns",
        source_column="sr_return_ship_cost|wr_return_ship_cost|cr_return_ship_cost",
    ),
    _fact_column(
        "refunded_cash",
        DataType.DECIMAL,
        source_table="store_returns|web_returns|catalog_returns",
        source_column="sr_refunded_cash|wr_refunded_cash|cr_refunded_cash",
    ),
    _fact_column(
        "reversed_charge",
        DataType.DECIMAL,
        source_table="store_returns|web_returns|catalog_returns",
        source_column="sr_reversed_charge|wr_reversed_charge|cr_reversed_charge",
    ),
    _fact_column(
        "account_credit",
        DataType.DECIMAL,
        source_table="web_returns",
        source_column="wr_account_credit",
    ),
    _fact_column(
        "store_credit",
        DataType.DECIMAL,
        source_table="store_returns|catalog_returns",
        source_column="sr_store_credit|cr_store_credit",
    ),
    _fact_column(
        "return_net_loss",
        DataType.DECIMAL,
        source_table="store_returns|web_returns|catalog_returns",
        source_column="sr_net_loss|wr_net_loss|cr_net_loss",
    ),
    _fact_column(
        "has_return",
        DataType.CHAR,
        size=1,
        nullable=False,
        description="Y/N flag set when a matching return exists for the sale",
    ),
)


def _prefixed_columns(role: DimensionRole) -> list[OBTColumn]:
    """Clone dimension columns with the provided prefix and role metadata."""
    prefixed: list[OBTColumn] = []
    for column in role.table.columns:
        prefixed.append(
            OBTColumn(
                name=f"{role.prefix}{column.name.lower()}",
                data_type=column.data_type,
                size=column.size,
                nullable=True,
                source_table=role.table.name,
                source_column=column.name,
                role=role.name,
                description=role.description,
            )
        )
    return prefixed


# Full set of dimension roles to inline
DIMENSION_ROLES_FULL: tuple[DimensionRole, ...] = (
    DimensionRole("sold_date", DATE_DIM, "sold_date_", "Sold date attributes"),
    DimensionRole("sold_time", TIME_DIM, "sold_time_", "Sold time attributes"),
    DimensionRole("ship_date", DATE_DIM, "ship_date_", "Shipment date attributes"),
    DimensionRole("return_date", DATE_DIM, "return_date_", "Return date attributes"),
    DimensionRole("return_time", TIME_DIM, "return_time_", "Return time attributes"),
    DimensionRole("item", ITEM, "item_", "Item attributes"),
    DimensionRole("promotion", PROMOTION, "promo_", "Promotion attributes"),
    DimensionRole("reason", REASON, "reason_", "Return reason attributes"),
    DimensionRole("store", STORE, "store_", "Store attributes"),
    DimensionRole("web_site", WEB_SITE, "web_site_", "Web site attributes"),
    DimensionRole("web_page", WEB_PAGE, "web_page_", "Web page attributes"),
    DimensionRole("call_center", CALL_CENTER, "call_center_", "Call center attributes"),
    DimensionRole("catalog_page", CATALOG_PAGE, "catalog_page_", "Catalog page attributes"),
    DimensionRole("ship_mode", SHIP_MODE, "ship_mode_", "Ship mode attributes"),
    DimensionRole("warehouse", WAREHOUSE, "warehouse_", "Warehouse attributes"),
    DimensionRole("bill_customer", CUSTOMER, "bill_customer_", "Billing customer attributes"),
    DimensionRole("ship_customer", CUSTOMER, "ship_customer_", "Shipping customer attributes"),
    DimensionRole("returning_customer", CUSTOMER, "returning_customer_", "Returning customer attributes"),
    DimensionRole("refunded_customer", CUSTOMER, "refunded_customer_", "Refunded customer attributes"),
    DimensionRole(
        "bill_cdemo",
        CUSTOMER_DEMOGRAPHICS,
        "bill_cdemo_",
        "Billing customer demographics attributes",
    ),
    DimensionRole(
        "ship_cdemo",
        CUSTOMER_DEMOGRAPHICS,
        "ship_cdemo_",
        "Shipping customer demographics attributes",
    ),
    DimensionRole(
        "returning_cdemo",
        CUSTOMER_DEMOGRAPHICS,
        "returning_cdemo_",
        "Returning customer demographics attributes",
    ),
    DimensionRole(
        "refunded_cdemo",
        CUSTOMER_DEMOGRAPHICS,
        "refunded_cdemo_",
        "Refunded customer demographics attributes",
    ),
    DimensionRole(
        "bill_hdemo",
        HOUSEHOLD_DEMOGRAPHICS,
        "bill_hdemo_",
        "Billing household demographics attributes",
    ),
    DimensionRole(
        "ship_hdemo",
        HOUSEHOLD_DEMOGRAPHICS,
        "ship_hdemo_",
        "Shipping household demographics attributes",
    ),
    DimensionRole(
        "returning_hdemo",
        HOUSEHOLD_DEMOGRAPHICS,
        "returning_hdemo_",
        "Returning household demographics attributes",
    ),
    DimensionRole(
        "refunded_hdemo",
        HOUSEHOLD_DEMOGRAPHICS,
        "refunded_hdemo_",
        "Refunded household demographics attributes",
    ),
    DimensionRole("bill_address", CUSTOMER_ADDRESS, "bill_addr_", "Billing address attributes"),
    DimensionRole("ship_address", CUSTOMER_ADDRESS, "ship_addr_", "Shipping address attributes"),
    DimensionRole("returning_address", CUSTOMER_ADDRESS, "returning_addr_", "Returning address attributes"),
    DimensionRole("refunded_address", CUSTOMER_ADDRESS, "refunded_addr_", "Refunded address attributes"),
)

MINIMAL_DIMENSION_ROLE_NAMES = {
    "sold_date",
    "sold_time",
    "ship_date",
    "return_date",
    "return_time",
    "item",
    "promotion",
    "reason",
    "store",
    "web_site",
    "web_page",
    "call_center",
    "catalog_page",
    "ship_mode",
    "warehouse",
    "bill_customer",
    "ship_customer",
    "bill_cdemo",
    "ship_cdemo",
    "bill_hdemo",
    "ship_hdemo",
    "bill_address",
    "ship_address",
}

# Income band columns are derived from the income_band dimension
# which is accessed through household_demographics.hd_income_band_sk
# These columns extend the household_demographics roles with income bounds
INCOME_BAND_COLUMNS: tuple[OBTColumn, ...] = (
    OBTColumn(
        name="bill_hdemo_ib_lower_bound",
        data_type=DataType.INTEGER,
        nullable=True,
        source_table="income_band",
        source_column="ib_lower_bound",
        role="bill_hdemo",
        description="Billing household income band lower bound",
    ),
    OBTColumn(
        name="bill_hdemo_ib_upper_bound",
        data_type=DataType.INTEGER,
        nullable=True,
        source_table="income_band",
        source_column="ib_upper_bound",
        role="bill_hdemo",
        description="Billing household income band upper bound",
    ),
    OBTColumn(
        name="ship_hdemo_ib_lower_bound",
        data_type=DataType.INTEGER,
        nullable=True,
        source_table="income_band",
        source_column="ib_lower_bound",
        role="ship_hdemo",
        description="Shipping household income band lower bound",
    ),
    OBTColumn(
        name="ship_hdemo_ib_upper_bound",
        data_type=DataType.INTEGER,
        nullable=True,
        source_table="income_band",
        source_column="ib_upper_bound",
        role="ship_hdemo",
        description="Shipping household income band upper bound",
    ),
    OBTColumn(
        name="returning_hdemo_ib_lower_bound",
        data_type=DataType.INTEGER,
        nullable=True,
        source_table="income_band",
        source_column="ib_lower_bound",
        role="returning_hdemo",
        description="Returning household income band lower bound",
    ),
    OBTColumn(
        name="returning_hdemo_ib_upper_bound",
        data_type=DataType.INTEGER,
        nullable=True,
        source_table="income_band",
        source_column="ib_upper_bound",
        role="returning_hdemo",
        description="Returning household income band upper bound",
    ),
    OBTColumn(
        name="refunded_hdemo_ib_lower_bound",
        data_type=DataType.INTEGER,
        nullable=True,
        source_table="income_band",
        source_column="ib_lower_bound",
        role="refunded_hdemo",
        description="Refunded household income band lower bound",
    ),
    OBTColumn(
        name="refunded_hdemo_ib_upper_bound",
        data_type=DataType.INTEGER,
        nullable=True,
        source_table="income_band",
        source_column="ib_upper_bound",
        role="refunded_hdemo",
        description="Refunded household income band upper bound",
    ),
)

# Minimal mode income band columns (only bill and ship roles)
MINIMAL_INCOME_BAND_ROLES = {"bill_hdemo", "ship_hdemo"}


def _build_dimension_columns(role_filter: Iterable[DimensionRole]) -> list[OBTColumn]:
    """Materialize prefixed columns for the provided roles."""
    columns: list[OBTColumn] = []
    for role in role_filter:
        columns.extend(_prefixed_columns(role))
    return columns


@cache
def get_obt_columns(mode: str = DEFAULT_MODE) -> tuple[OBTColumn, ...]:
    """Return OBT columns for the requested mode."""
    mode_lower = mode.lower()
    if mode_lower not in ALLOWED_MODES:
        raise ValueError(f"Unsupported mode '{mode}'. Allowed modes: {sorted(ALLOWED_MODES)}")

    if mode_lower == "minimal":
        roles = [r for r in DIMENSION_ROLES_FULL if r.name in MINIMAL_DIMENSION_ROLE_NAMES]
        income_band_cols = [c for c in INCOME_BAND_COLUMNS if c.role in MINIMAL_INCOME_BAND_ROLES]
    else:
        roles = list(DIMENSION_ROLES_FULL)
        income_band_cols = list(INCOME_BAND_COLUMNS)

    columns: list[OBTColumn] = list(CORE_FACT_COLUMNS) + _build_dimension_columns(roles) + income_band_cols

    # Ensure unique column names to avoid ambiguous projections
    names = [col.name for col in columns]
    name_counts = Counter(names)
    duplicates = sorted(name for name, count in name_counts.items() if count > 1)
    if duplicates:
        raise ValueError(f"Duplicate column names detected in OBT schema: {duplicates}")

    return tuple(columns)


@cache
def get_obt_table(mode: str = DEFAULT_MODE) -> OBTTable:
    """Return the OBT table definition for the requested mode."""
    return OBTTable(name=OBT_TABLE_NAME, columns=get_obt_columns(mode))


def get_column_lineage(mode: str = DEFAULT_MODE) -> dict[str, dict[str, str | None]]:
    """Expose a mapping of OBT column name to source metadata."""
    lineage: dict[str, dict[str, str | None]] = {}
    for col in get_obt_columns(mode):
        lineage[col.name] = {
            "source_table": col.source_table,
            "source_column": col.source_column,
            "role": col.role,
            "description": col.description,
        }
    return lineage


# Default table definition using the full column set
TPCDS_OBT_TABLE = get_obt_table()

__all__ = [
    "ALLOWED_MODES",
    "DEFAULT_MODE",
    "OBTColumn",
    "OBT_TABLE_NAME",
    "TPCDS_OBT_TABLE",
    "get_column_lineage",
    "get_obt_columns",
    "get_obt_table",
]
