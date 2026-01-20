"""Declarative table definitions for the TPC-DS schema."""

from __future__ import annotations

from .models import Column, DataType, Table

STORE = Table(
    "store",
    [
        Column("s_store_sk", DataType.INTEGER, primary_key=True),
        Column("s_store_id", DataType.CHAR, size=16, nullable=False),
        Column("s_rec_start_date", DataType.DATE, nullable=True),
        Column("s_rec_end_date", DataType.DATE, nullable=True),
        Column("s_closed_date_sk", DataType.INTEGER, nullable=True),
        Column("s_store_name", DataType.VARCHAR, size=50, nullable=True),
        Column("s_number_employees", DataType.INTEGER, nullable=True),
        Column("s_floor_space", DataType.INTEGER, nullable=True),
        Column("s_hours", DataType.CHAR, size=20, nullable=True),
        Column("s_manager", DataType.VARCHAR, size=40, nullable=True),
        Column("s_market_id", DataType.INTEGER, nullable=True),
        Column("s_geography_class", DataType.VARCHAR, size=100, nullable=True),
        Column("s_market_desc", DataType.VARCHAR, size=100, nullable=True),
        Column("s_market_manager", DataType.VARCHAR, size=40, nullable=True),
        Column("s_division_id", DataType.INTEGER, nullable=True),
        Column("s_division_name", DataType.VARCHAR, size=50, nullable=True),
        Column("s_company_id", DataType.INTEGER, nullable=True),
        Column("s_company_name", DataType.VARCHAR, size=50, nullable=True),
        Column("s_street_number", DataType.VARCHAR, size=10, nullable=True),
        Column("s_street_name", DataType.VARCHAR, size=60, nullable=True),
        Column("s_street_type", DataType.CHAR, size=15, nullable=True),
        Column("s_suite_number", DataType.CHAR, size=10, nullable=True),
        Column("s_city", DataType.VARCHAR, size=60, nullable=True),
        Column("s_county", DataType.VARCHAR, size=30, nullable=True),
        Column("s_state", DataType.CHAR, size=2, nullable=True),
        Column("s_zip", DataType.CHAR, size=10, nullable=True),
        Column("s_country", DataType.VARCHAR, size=20, nullable=True),
        Column("s_gmt_offset", DataType.DECIMAL, nullable=True),
        Column("s_tax_percentage", DataType.DECIMAL, nullable=True),
    ],
)

# Date Dimension
DATE_DIM = Table(
    "date_dim",
    [
        Column("d_date_sk", DataType.INTEGER, primary_key=True),
        Column("d_date_id", DataType.CHAR, size=16, nullable=False),
        Column("d_date", DataType.DATE, nullable=False),
        Column("d_month_seq", DataType.INTEGER, nullable=True),
        Column("d_week_seq", DataType.INTEGER, nullable=True),
        Column("d_quarter_seq", DataType.INTEGER, nullable=True),
        Column("d_year", DataType.INTEGER, nullable=True),
        Column("d_dow", DataType.INTEGER, nullable=True),
        Column("d_moy", DataType.INTEGER, nullable=True),
        Column("d_dom", DataType.INTEGER, nullable=True),
        Column("d_qoy", DataType.INTEGER, nullable=True),
        Column("d_fy_year", DataType.INTEGER, nullable=True),
        Column("d_fy_quarter_seq", DataType.INTEGER, nullable=True),
        Column("d_fy_week_seq", DataType.INTEGER, nullable=True),
        Column("d_day_name", DataType.CHAR, size=9, nullable=True),
        Column("d_quarter_name", DataType.CHAR, size=6, nullable=True),
        Column("d_holiday", DataType.CHAR, size=1, nullable=True),
        Column("d_weekend", DataType.CHAR, size=1, nullable=True),
        Column("d_following_holiday", DataType.CHAR, size=1, nullable=True),
        Column("d_first_dom", DataType.INTEGER, nullable=True),
        Column("d_last_dom", DataType.INTEGER, nullable=True),
        Column("d_same_day_ly", DataType.INTEGER, nullable=True),
        Column("d_same_day_lq", DataType.INTEGER, nullable=True),
        Column("d_current_day", DataType.CHAR, size=1, nullable=True),
        Column("d_current_week", DataType.CHAR, size=1, nullable=True),
        Column("d_current_month", DataType.CHAR, size=1, nullable=True),
        Column("d_current_quarter", DataType.CHAR, size=1, nullable=True),
        Column("d_current_year", DataType.CHAR, size=1, nullable=True),
    ],
)

# Time Dimension
TIME_DIM = Table(
    "time_dim",
    [
        Column("t_time_sk", DataType.INTEGER, primary_key=True),
        Column("t_time_id", DataType.CHAR, size=16, nullable=False),
        Column("t_time", DataType.INTEGER, nullable=True),
        Column("t_hour", DataType.INTEGER, nullable=True),
        Column("t_minute", DataType.INTEGER, nullable=True),
        Column("t_second", DataType.INTEGER, nullable=True),
        Column("t_am_pm", DataType.CHAR, size=2, nullable=True),
        Column("t_shift", DataType.CHAR, size=20, nullable=True),
        Column("t_sub_shift", DataType.CHAR, size=20, nullable=True),
        Column("t_meal_time", DataType.CHAR, size=20, nullable=True),
    ],
)

# Item Dimension
ITEM = Table(
    "item",
    [
        Column("i_item_sk", DataType.INTEGER, primary_key=True),
        Column("i_item_id", DataType.CHAR, size=16, nullable=False),
        Column("i_rec_start_date", DataType.DATE, nullable=True),
        Column("i_rec_end_date", DataType.DATE, nullable=True),
        Column("i_item_desc", DataType.VARCHAR, size=200, nullable=True),
        Column("i_current_price", DataType.DECIMAL, nullable=True),
        Column("i_wholesale_cost", DataType.DECIMAL, nullable=True),
        Column("i_brand_id", DataType.INTEGER, nullable=True),
        Column("i_brand", DataType.CHAR, size=50, nullable=True),
        Column("i_class_id", DataType.INTEGER, nullable=True),
        Column("i_class", DataType.CHAR, size=50, nullable=True),
        Column("i_category_id", DataType.INTEGER, nullable=True),
        Column("i_category", DataType.CHAR, size=50, nullable=True),
        Column("i_manufact_id", DataType.INTEGER, nullable=True),
        Column("i_manufact", DataType.CHAR, size=50, nullable=True),
        Column("i_size", DataType.CHAR, size=20, nullable=True),
        Column("i_formulation", DataType.CHAR, size=20, nullable=True),
        Column("i_color", DataType.CHAR, size=20, nullable=True),
        Column("i_units", DataType.CHAR, size=10, nullable=True),
        Column("i_container", DataType.CHAR, size=10, nullable=True),
        Column("i_manager_id", DataType.INTEGER, nullable=True),
        Column("i_product_name", DataType.CHAR, size=50, nullable=True),
    ],
)

# Customer Dimension
CUSTOMER = Table(
    "customer",
    [
        Column("c_customer_sk", DataType.INTEGER, primary_key=True),
        Column("c_customer_id", DataType.CHAR, size=16, nullable=False),
        Column("c_current_cdemo_sk", DataType.INTEGER, nullable=True),
        Column("c_current_hdemo_sk", DataType.INTEGER, nullable=True),
        Column("c_current_addr_sk", DataType.INTEGER, nullable=True),
        Column("c_first_shipto_date_sk", DataType.INTEGER, nullable=True),
        Column("c_first_sales_date_sk", DataType.INTEGER, nullable=True),
        Column("c_salutation", DataType.CHAR, size=10, nullable=True),
        Column("c_first_name", DataType.CHAR, size=20, nullable=True),
        Column("c_last_name", DataType.CHAR, size=30, nullable=True),
        Column("c_preferred_cust_flag", DataType.CHAR, size=1, nullable=True),
        Column("c_birth_day", DataType.INTEGER, nullable=True),
        Column("c_birth_month", DataType.INTEGER, nullable=True),
        Column("c_birth_year", DataType.INTEGER, nullable=True),
        Column("c_birth_country", DataType.VARCHAR, size=20, nullable=True),
        Column("c_login", DataType.CHAR, size=13, nullable=True),
        Column("c_email_address", DataType.CHAR, size=50, nullable=True),
        Column("c_last_review_date_sk", DataType.INTEGER, nullable=True),
    ],
)

# Customer Demographics
CUSTOMER_DEMOGRAPHICS = Table(
    "customer_demographics",
    [
        Column("cd_demo_sk", DataType.INTEGER, primary_key=True),
        Column("cd_gender", DataType.CHAR, size=1, nullable=True),
        Column("cd_marital_status", DataType.CHAR, size=1, nullable=True),
        Column("cd_education_status", DataType.CHAR, size=20, nullable=True),
        Column("cd_purchase_estimate", DataType.INTEGER, nullable=True),
        Column("cd_credit_rating", DataType.CHAR, size=10, nullable=True),
        Column("cd_dep_count", DataType.INTEGER, nullable=True),
        Column("cd_dep_employed_count", DataType.INTEGER, nullable=True),
        Column("cd_dep_college_count", DataType.INTEGER, nullable=True),
    ],
)

# Household Demographics
HOUSEHOLD_DEMOGRAPHICS = Table(
    "household_demographics",
    [
        Column("hd_demo_sk", DataType.INTEGER, primary_key=True),
        Column("hd_income_band_sk", DataType.INTEGER, nullable=True),
        Column("hd_buy_potential", DataType.CHAR, size=15, nullable=True),
        Column("hd_dep_count", DataType.INTEGER, nullable=True),
        Column("hd_vehicle_count", DataType.INTEGER, nullable=True),
    ],
)

# Income Band
INCOME_BAND = Table(
    "income_band",
    [
        Column("ib_income_band_sk", DataType.INTEGER, primary_key=True),
        Column("ib_lower_bound", DataType.INTEGER, nullable=True),
        Column("ib_upper_bound", DataType.INTEGER, nullable=True),
    ],
)

# Promotion
PROMOTION = Table(
    "promotion",
    [
        Column("p_promo_sk", DataType.INTEGER, primary_key=True),
        Column("p_promo_id", DataType.CHAR, size=16, nullable=False),
        Column("p_start_date_sk", DataType.INTEGER, nullable=True),
        Column("p_end_date_sk", DataType.INTEGER, nullable=True),
        Column("p_item_sk", DataType.INTEGER, nullable=True),
        Column("p_cost", DataType.DECIMAL, nullable=True),
        Column("p_response_target", DataType.INTEGER, nullable=True),
        Column("p_promo_name", DataType.CHAR, size=50, nullable=True),
        Column("p_channel_dmail", DataType.CHAR, size=1, nullable=True),
        Column("p_channel_email", DataType.CHAR, size=1, nullable=True),
        Column("p_channel_catalog", DataType.CHAR, size=1, nullable=True),
        Column("p_channel_tv", DataType.CHAR, size=1, nullable=True),
        Column("p_channel_radio", DataType.CHAR, size=1, nullable=True),
        Column("p_channel_press", DataType.CHAR, size=1, nullable=True),
        Column("p_channel_event", DataType.CHAR, size=1, nullable=True),
        Column("p_channel_demo", DataType.CHAR, size=1, nullable=True),
        Column("p_channel_details", DataType.VARCHAR, size=100, nullable=True),
        Column("p_purpose", DataType.CHAR, size=15, nullable=True),
        Column("p_discount_active", DataType.CHAR, size=1, nullable=True),
    ],
)

# Customer Address
CUSTOMER_ADDRESS = Table(
    "customer_address",
    [
        Column("ca_address_sk", DataType.INTEGER, primary_key=True),
        Column("ca_address_id", DataType.CHAR, size=16, nullable=False),
        Column("ca_street_number", DataType.CHAR, size=10, nullable=True),
        Column("ca_street_name", DataType.VARCHAR, size=60, nullable=True),
        Column("ca_street_type", DataType.CHAR, size=15, nullable=True),
        Column("ca_suite_number", DataType.CHAR, size=10, nullable=True),
        Column("ca_city", DataType.VARCHAR, size=60, nullable=True),
        Column("ca_county", DataType.VARCHAR, size=30, nullable=True),
        Column("ca_state", DataType.CHAR, size=2, nullable=True),
        Column("ca_zip", DataType.CHAR, size=10, nullable=True),
        Column("ca_country", DataType.VARCHAR, size=20, nullable=True),
        Column("ca_gmt_offset", DataType.DECIMAL, nullable=True),
        Column("ca_location_type", DataType.CHAR, size=20, nullable=True),
    ],
)

# Warehouse
WAREHOUSE = Table(
    "warehouse",
    [
        Column("w_warehouse_sk", DataType.INTEGER, primary_key=True),
        Column("w_warehouse_id", DataType.CHAR, size=16, nullable=False),
        Column("w_warehouse_name", DataType.VARCHAR, size=20, nullable=True),
        Column("w_warehouse_sq_ft", DataType.INTEGER, nullable=True),
        Column("w_street_number", DataType.CHAR, size=10, nullable=True),
        Column("w_street_name", DataType.VARCHAR, size=60, nullable=True),
        Column("w_street_type", DataType.CHAR, size=15, nullable=True),
        Column("w_suite_number", DataType.CHAR, size=10, nullable=True),
        Column("w_city", DataType.VARCHAR, size=60, nullable=True),
        Column("w_county", DataType.VARCHAR, size=30, nullable=True),
        Column("w_state", DataType.CHAR, size=2, nullable=True),
        Column("w_zip", DataType.CHAR, size=10, nullable=True),
        Column("w_country", DataType.VARCHAR, size=20, nullable=True),
        Column("w_gmt_offset", DataType.DECIMAL, nullable=True),
    ],
)

# Web Site
WEB_SITE = Table(
    "web_site",
    [
        Column("web_site_sk", DataType.INTEGER, primary_key=True),
        Column("web_site_id", DataType.CHAR, size=16, nullable=False),
        Column("web_rec_start_date", DataType.DATE, nullable=True),
        Column("web_rec_end_date", DataType.DATE, nullable=True),
        Column("web_name", DataType.VARCHAR, size=50, nullable=True),
        Column("web_open_date_sk", DataType.INTEGER, nullable=True),
        Column("web_close_date_sk", DataType.INTEGER, nullable=True),
        Column("web_class", DataType.VARCHAR, size=50, nullable=True),
        Column("web_manager", DataType.VARCHAR, size=40, nullable=True),
        Column("web_mkt_id", DataType.INTEGER, nullable=True),
        Column("web_mkt_class", DataType.VARCHAR, size=50, nullable=True),
        Column("web_mkt_desc", DataType.VARCHAR, size=100, nullable=True),
        Column("web_market_manager", DataType.VARCHAR, size=40, nullable=True),
        Column("web_company_id", DataType.INTEGER, nullable=True),
        Column("web_company_name", DataType.CHAR, size=50, nullable=True),
        Column("web_street_number", DataType.CHAR, size=10, nullable=True),
        Column("web_street_name", DataType.VARCHAR, size=60, nullable=True),
        Column("web_street_type", DataType.CHAR, size=15, nullable=True),
        Column("web_suite_number", DataType.CHAR, size=10, nullable=True),
        Column("web_city", DataType.VARCHAR, size=60, nullable=True),
        Column("web_county", DataType.VARCHAR, size=30, nullable=True),
        Column("web_state", DataType.CHAR, size=2, nullable=True),
        Column("web_zip", DataType.CHAR, size=10, nullable=True),
        Column("web_country", DataType.VARCHAR, size=20, nullable=True),
        Column("web_gmt_offset", DataType.DECIMAL, nullable=True),
        Column("web_tax_percentage", DataType.DECIMAL, nullable=True),
    ],
)

# Web Page
WEB_PAGE = Table(
    "web_page",
    [
        Column("wp_web_page_sk", DataType.INTEGER, primary_key=True),
        Column("wp_web_page_id", DataType.CHAR, size=16, nullable=False),
        Column("wp_rec_start_date", DataType.DATE, nullable=True),
        Column("wp_rec_end_date", DataType.DATE, nullable=True),
        Column("wp_creation_date_sk", DataType.INTEGER, nullable=True),
        Column("wp_access_date_sk", DataType.INTEGER, nullable=True),
        Column("wp_autogen_flag", DataType.CHAR, size=1, nullable=True),
        Column("wp_customer_sk", DataType.INTEGER, nullable=True),
        Column("wp_url", DataType.VARCHAR, size=100, nullable=True),
        Column("wp_type", DataType.CHAR, size=50, nullable=True),
        Column("wp_char_count", DataType.INTEGER, nullable=True),
        Column("wp_link_count", DataType.INTEGER, nullable=True),
        Column("wp_image_count", DataType.INTEGER, nullable=True),
        Column("wp_max_ad_count", DataType.INTEGER, nullable=True),
    ],
)

# Reason
REASON = Table(
    "reason",
    [
        Column("r_reason_sk", DataType.INTEGER, primary_key=True),
        Column("r_reason_id", DataType.CHAR, size=16, nullable=False),
        Column("r_reason_desc", DataType.CHAR, size=100, nullable=True),
    ],
)

# Call Center
CALL_CENTER = Table(
    "call_center",
    [
        Column("cc_call_center_sk", DataType.INTEGER, primary_key=True),
        Column("cc_call_center_id", DataType.CHAR, size=16, nullable=False),
        Column("cc_rec_start_date", DataType.DATE, nullable=True),
        Column("cc_rec_end_date", DataType.DATE, nullable=True),
        Column("cc_closed_date_sk", DataType.INTEGER, nullable=True),
        Column("cc_open_date_sk", DataType.INTEGER, nullable=True),
        Column("cc_name", DataType.VARCHAR, size=50, nullable=True),
        Column("cc_class", DataType.VARCHAR, size=50, nullable=True),
        Column("cc_employees", DataType.INTEGER, nullable=True),
        Column("cc_sq_ft", DataType.INTEGER, nullable=True),
        Column("cc_hours", DataType.CHAR, size=20, nullable=True),
        Column("cc_manager", DataType.VARCHAR, size=40, nullable=True),
        Column("cc_mkt_id", DataType.INTEGER, nullable=True),
        Column("cc_mkt_class", DataType.CHAR, size=50, nullable=True),
        Column("cc_mkt_desc", DataType.VARCHAR, size=100, nullable=True),
        Column("cc_market_manager", DataType.VARCHAR, size=40, nullable=True),
        Column("cc_division", DataType.INTEGER, nullable=True),
        Column("cc_division_name", DataType.VARCHAR, size=50, nullable=True),
        Column("cc_company", DataType.INTEGER, nullable=True),
        Column("cc_company_name", DataType.CHAR, size=50, nullable=True),
        Column("cc_street_number", DataType.CHAR, size=10, nullable=True),
        Column("cc_street_name", DataType.VARCHAR, size=60, nullable=True),
        Column("cc_street_type", DataType.CHAR, size=15, nullable=True),
        Column("cc_suite_number", DataType.CHAR, size=10, nullable=True),
        Column("cc_city", DataType.VARCHAR, size=60, nullable=True),
        Column("cc_county", DataType.VARCHAR, size=30, nullable=True),
        Column("cc_state", DataType.CHAR, size=2, nullable=True),
        Column("cc_zip", DataType.CHAR, size=10, nullable=True),
        Column("cc_country", DataType.VARCHAR, size=20, nullable=True),
        Column("cc_gmt_offset", DataType.DECIMAL, nullable=True),
        Column("cc_tax_percentage", DataType.DECIMAL, nullable=True),
    ],
)

# Catalog Page
CATALOG_PAGE = Table(
    "catalog_page",
    [
        Column("cp_catalog_page_sk", DataType.INTEGER, primary_key=True),
        Column("cp_catalog_page_id", DataType.CHAR, size=16, nullable=False),
        Column("cp_start_date_sk", DataType.INTEGER, nullable=True),
        Column("cp_end_date_sk", DataType.INTEGER, nullable=True),
        Column("cp_department", DataType.VARCHAR, size=50, nullable=True),
        Column("cp_catalog_number", DataType.INTEGER, nullable=True),
        Column("cp_catalog_page_number", DataType.INTEGER, nullable=True),
        Column("cp_description", DataType.VARCHAR, size=100, nullable=True),
        Column("cp_type", DataType.VARCHAR, size=100, nullable=True),
    ],
)

# Ship Mode
SHIP_MODE = Table(
    "ship_mode",
    [
        Column("sm_ship_mode_sk", DataType.INTEGER, primary_key=True),
        Column("sm_ship_mode_id", DataType.CHAR, size=16, nullable=False),
        Column("sm_type", DataType.CHAR, size=30, nullable=True),
        Column("sm_code", DataType.CHAR, size=10, nullable=True),
        Column("sm_carrier", DataType.CHAR, size=20, nullable=True),
        Column("sm_contract", DataType.CHAR, size=20, nullable=True),
    ],
)

# Fact Tables
STORE_SALES = Table(
    "store_sales",
    [
        Column(
            "ss_sold_date_sk",
            DataType.INTEGER,
            nullable=True,
            foreign_key=("date_dim", "d_date_sk"),
        ),
        Column(
            "ss_sold_time_sk",
            DataType.INTEGER,
            nullable=True,
            foreign_key=("time_dim", "t_time_sk"),
        ),
        Column(
            "ss_item_sk",
            DataType.INTEGER,
            nullable=False,
            foreign_key=("item", "i_item_sk"),
        ),
        Column(
            "ss_customer_sk",
            DataType.INTEGER,
            nullable=True,
            foreign_key=("customer", "c_customer_sk"),
        ),
        Column(
            "ss_cdemo_sk",
            DataType.INTEGER,
            nullable=True,
            foreign_key=("customer_demographics", "cd_demo_sk"),
        ),
        Column(
            "ss_hdemo_sk",
            DataType.INTEGER,
            nullable=True,
            foreign_key=("household_demographics", "hd_demo_sk"),
        ),
        Column(
            "ss_addr_sk",
            DataType.INTEGER,
            nullable=True,
            foreign_key=("customer_address", "ca_address_sk"),
        ),
        Column(
            "ss_store_sk",
            DataType.INTEGER,
            nullable=True,
            foreign_key=("store", "s_store_sk"),
        ),
        Column(
            "ss_promo_sk",
            DataType.INTEGER,
            nullable=True,
            foreign_key=("promotion", "p_promo_sk"),
        ),
        Column("ss_ticket_number", DataType.INTEGER, primary_key=True),
        Column("ss_quantity", DataType.INTEGER, nullable=True),
        Column("ss_wholesale_cost", DataType.DECIMAL, nullable=True),
        Column("ss_list_price", DataType.DECIMAL, nullable=True),
        Column("ss_sales_price", DataType.DECIMAL, nullable=True),
        Column("ss_ext_discount_amt", DataType.DECIMAL, nullable=True),
        Column("ss_ext_sales_price", DataType.DECIMAL, nullable=True),
        Column("ss_ext_wholesale_cost", DataType.DECIMAL, nullable=True),
        Column("ss_ext_list_price", DataType.DECIMAL, nullable=True),
        Column("ss_ext_tax", DataType.DECIMAL, nullable=True),
        Column("ss_coupon_amt", DataType.DECIMAL, nullable=True),
        Column("ss_net_paid", DataType.DECIMAL, nullable=True),
        Column("ss_net_paid_inc_tax", DataType.DECIMAL, nullable=True),
        Column("ss_net_profit", DataType.DECIMAL, nullable=True),
    ],
)

STORE_RETURNS = Table(
    "store_returns",
    [
        Column(
            "sr_returned_date_sk",
            DataType.INTEGER,
            nullable=True,
            foreign_key=("date_dim", "d_date_sk"),
        ),
        Column(
            "sr_return_time_sk",
            DataType.INTEGER,
            nullable=True,
            foreign_key=("time_dim", "t_time_sk"),
        ),
        Column(
            "sr_item_sk",
            DataType.INTEGER,
            nullable=False,
            foreign_key=("item", "i_item_sk"),
        ),
        Column(
            "sr_customer_sk",
            DataType.INTEGER,
            nullable=True,
            foreign_key=("customer", "c_customer_sk"),
        ),
        Column(
            "sr_cdemo_sk",
            DataType.INTEGER,
            nullable=True,
            foreign_key=("customer_demographics", "cd_demo_sk"),
        ),
        Column(
            "sr_hdemo_sk",
            DataType.INTEGER,
            nullable=True,
            foreign_key=("household_demographics", "hd_demo_sk"),
        ),
        Column(
            "sr_addr_sk",
            DataType.INTEGER,
            nullable=True,
            foreign_key=("customer_address", "ca_address_sk"),
        ),
        Column(
            "sr_store_sk",
            DataType.INTEGER,
            nullable=True,
            foreign_key=("store", "s_store_sk"),
        ),
        Column(
            "sr_reason_sk",
            DataType.INTEGER,
            nullable=True,
            foreign_key=("reason", "r_reason_sk"),
        ),
        Column(
            "sr_ticket_number",
            DataType.INTEGER,
            primary_key=True,
            foreign_key=("store_sales", "ss_ticket_number"),
        ),
        Column("sr_return_quantity", DataType.INTEGER, nullable=True),
        Column("sr_return_amt", DataType.DECIMAL, nullable=True),
        Column("sr_return_tax", DataType.DECIMAL, nullable=True),
        Column("sr_return_amt_inc_tax", DataType.DECIMAL, nullable=True),
        Column("sr_fee", DataType.DECIMAL, nullable=True),
        Column("sr_return_ship_cost", DataType.DECIMAL, nullable=True),
        Column("sr_refunded_cash", DataType.DECIMAL, nullable=True),
        Column("sr_reversed_charge", DataType.DECIMAL, nullable=True),
        Column("sr_store_credit", DataType.DECIMAL, nullable=True),
        Column("sr_net_loss", DataType.DECIMAL, nullable=True),
    ],
)

WEB_SALES = Table(
    "web_sales",
    [
        Column(
            "ws_sold_date_sk",
            DataType.INTEGER,
            nullable=True,
            foreign_key=("date_dim", "d_date_sk"),
        ),
        Column(
            "ws_sold_time_sk",
            DataType.INTEGER,
            nullable=True,
            foreign_key=("time_dim", "t_time_sk"),
        ),
        Column(
            "ws_ship_date_sk",
            DataType.INTEGER,
            nullable=True,
            foreign_key=("date_dim", "d_date_sk"),
        ),
        Column(
            "ws_item_sk",
            DataType.INTEGER,
            nullable=False,
            foreign_key=("item", "i_item_sk"),
        ),
        Column(
            "ws_bill_customer_sk",
            DataType.INTEGER,
            nullable=True,
            foreign_key=("customer", "c_customer_sk"),
        ),
        Column(
            "ws_bill_cdemo_sk",
            DataType.INTEGER,
            nullable=True,
            foreign_key=("customer_demographics", "cd_demo_sk"),
        ),
        Column(
            "ws_bill_hdemo_sk",
            DataType.INTEGER,
            nullable=True,
            foreign_key=("household_demographics", "hd_demo_sk"),
        ),
        Column(
            "ws_bill_addr_sk",
            DataType.INTEGER,
            nullable=True,
            foreign_key=("customer_address", "ca_address_sk"),
        ),
        Column(
            "ws_ship_customer_sk",
            DataType.INTEGER,
            nullable=True,
            foreign_key=("customer", "c_customer_sk"),
        ),
        Column(
            "ws_ship_cdemo_sk",
            DataType.INTEGER,
            nullable=True,
            foreign_key=("customer_demographics", "cd_demo_sk"),
        ),
        Column(
            "ws_ship_hdemo_sk",
            DataType.INTEGER,
            nullable=True,
            foreign_key=("household_demographics", "hd_demo_sk"),
        ),
        Column(
            "ws_ship_addr_sk",
            DataType.INTEGER,
            nullable=True,
            foreign_key=("customer_address", "ca_address_sk"),
        ),
        Column(
            "ws_web_page_sk",
            DataType.INTEGER,
            nullable=True,
            foreign_key=("web_page", "wp_web_page_sk"),
        ),
        Column(
            "ws_web_site_sk",
            DataType.INTEGER,
            nullable=True,
            foreign_key=("web_site", "web_site_sk"),
        ),
        Column(
            "ws_ship_mode_sk",
            DataType.INTEGER,
            nullable=True,
            foreign_key=("ship_mode", "sm_ship_mode_sk"),
        ),
        Column(
            "ws_warehouse_sk",
            DataType.INTEGER,
            nullable=True,
            foreign_key=("warehouse", "w_warehouse_sk"),
        ),
        Column(
            "ws_promo_sk",
            DataType.INTEGER,
            nullable=True,
            foreign_key=("promotion", "p_promo_sk"),
        ),
        Column("ws_order_number", DataType.INTEGER, primary_key=True),
        Column("ws_quantity", DataType.INTEGER, nullable=True),
        Column("ws_wholesale_cost", DataType.DECIMAL, nullable=True),
        Column("ws_list_price", DataType.DECIMAL, nullable=True),
        Column("ws_sales_price", DataType.DECIMAL, nullable=True),
        Column("ws_ext_discount_amt", DataType.DECIMAL, nullable=True),
        Column("ws_ext_sales_price", DataType.DECIMAL, nullable=True),
        Column("ws_ext_wholesale_cost", DataType.DECIMAL, nullable=True),
        Column("ws_ext_list_price", DataType.DECIMAL, nullable=True),
        Column("ws_ext_tax", DataType.DECIMAL, nullable=True),
        Column("ws_coupon_amt", DataType.DECIMAL, nullable=True),
        Column("ws_ext_ship_cost", DataType.DECIMAL, nullable=True),
        Column("ws_net_paid", DataType.DECIMAL, nullable=True),
        Column("ws_net_paid_inc_tax", DataType.DECIMAL, nullable=True),
        Column("ws_net_paid_inc_ship", DataType.DECIMAL, nullable=True),
        Column("ws_net_paid_inc_ship_tax", DataType.DECIMAL, nullable=True),
        Column("ws_net_profit", DataType.DECIMAL, nullable=True),
    ],
)

WEB_RETURNS = Table(
    "web_returns",
    [
        Column(
            "wr_returned_date_sk",
            DataType.INTEGER,
            nullable=True,
            foreign_key=("date_dim", "d_date_sk"),
        ),
        Column(
            "wr_returned_time_sk",
            DataType.INTEGER,
            nullable=True,
            foreign_key=("time_dim", "t_time_sk"),
        ),
        Column(
            "wr_item_sk",
            DataType.INTEGER,
            nullable=False,
            foreign_key=("item", "i_item_sk"),
        ),
        Column(
            "wr_refunded_customer_sk",
            DataType.INTEGER,
            nullable=True,
            foreign_key=("customer", "c_customer_sk"),
        ),
        Column(
            "wr_refunded_cdemo_sk",
            DataType.INTEGER,
            nullable=True,
            foreign_key=("customer_demographics", "cd_demo_sk"),
        ),
        Column(
            "wr_refunded_hdemo_sk",
            DataType.INTEGER,
            nullable=True,
            foreign_key=("household_demographics", "hd_demo_sk"),
        ),
        Column(
            "wr_refunded_addr_sk",
            DataType.INTEGER,
            nullable=True,
            foreign_key=("customer_address", "ca_address_sk"),
        ),
        Column(
            "wr_returning_customer_sk",
            DataType.INTEGER,
            nullable=True,
            foreign_key=("customer", "c_customer_sk"),
        ),
        Column(
            "wr_returning_cdemo_sk",
            DataType.INTEGER,
            nullable=True,
            foreign_key=("customer_demographics", "cd_demo_sk"),
        ),
        Column(
            "wr_returning_hdemo_sk",
            DataType.INTEGER,
            nullable=True,
            foreign_key=("household_demographics", "hd_demo_sk"),
        ),
        Column(
            "wr_returning_addr_sk",
            DataType.INTEGER,
            nullable=True,
            foreign_key=("customer_address", "ca_address_sk"),
        ),
        Column(
            "wr_web_page_sk",
            DataType.INTEGER,
            nullable=True,
            foreign_key=("web_page", "wp_web_page_sk"),
        ),
        Column(
            "wr_reason_sk",
            DataType.INTEGER,
            nullable=True,
            foreign_key=("reason", "r_reason_sk"),
        ),
        Column(
            "wr_order_number",
            DataType.INTEGER,
            primary_key=True,
            foreign_key=("web_sales", "ws_order_number"),
        ),
        Column("wr_return_quantity", DataType.INTEGER, nullable=True),
        Column("wr_return_amt", DataType.DECIMAL, nullable=True),
        Column("wr_return_tax", DataType.DECIMAL, nullable=True),
        Column("wr_return_amt_inc_tax", DataType.DECIMAL, nullable=True),
        Column("wr_fee", DataType.DECIMAL, nullable=True),
        Column("wr_return_ship_cost", DataType.DECIMAL, nullable=True),
        Column("wr_refunded_cash", DataType.DECIMAL, nullable=True),
        Column("wr_reversed_charge", DataType.DECIMAL, nullable=True),
        Column("wr_account_credit", DataType.DECIMAL, nullable=True),
        Column("wr_net_loss", DataType.DECIMAL, nullable=True),
    ],
)

CATALOG_SALES = Table(
    "catalog_sales",
    [
        Column(
            "cs_sold_date_sk",
            DataType.INTEGER,
            nullable=True,
            foreign_key=("date_dim", "d_date_sk"),
        ),
        Column(
            "cs_sold_time_sk",
            DataType.INTEGER,
            nullable=True,
            foreign_key=("time_dim", "t_time_sk"),
        ),
        Column(
            "cs_ship_date_sk",
            DataType.INTEGER,
            nullable=True,
            foreign_key=("date_dim", "d_date_sk"),
        ),
        Column(
            "cs_bill_customer_sk",
            DataType.INTEGER,
            nullable=True,
            foreign_key=("customer", "c_customer_sk"),
        ),
        Column(
            "cs_bill_cdemo_sk",
            DataType.INTEGER,
            nullable=True,
            foreign_key=("customer_demographics", "cd_demo_sk"),
        ),
        Column(
            "cs_bill_hdemo_sk",
            DataType.INTEGER,
            nullable=True,
            foreign_key=("household_demographics", "hd_demo_sk"),
        ),
        Column(
            "cs_bill_addr_sk",
            DataType.INTEGER,
            nullable=True,
            foreign_key=("customer_address", "ca_address_sk"),
        ),
        Column(
            "cs_ship_customer_sk",
            DataType.INTEGER,
            nullable=True,
            foreign_key=("customer", "c_customer_sk"),
        ),
        Column(
            "cs_ship_cdemo_sk",
            DataType.INTEGER,
            nullable=True,
            foreign_key=("customer_demographics", "cd_demo_sk"),
        ),
        Column(
            "cs_ship_hdemo_sk",
            DataType.INTEGER,
            nullable=True,
            foreign_key=("household_demographics", "hd_demo_sk"),
        ),
        Column(
            "cs_ship_addr_sk",
            DataType.INTEGER,
            nullable=True,
            foreign_key=("customer_address", "ca_address_sk"),
        ),
        Column(
            "cs_call_center_sk",
            DataType.INTEGER,
            nullable=True,
            foreign_key=("call_center", "cc_call_center_sk"),
        ),
        Column(
            "cs_catalog_page_sk",
            DataType.INTEGER,
            nullable=True,
            foreign_key=("catalog_page", "cp_catalog_page_sk"),
        ),
        Column(
            "cs_ship_mode_sk",
            DataType.INTEGER,
            nullable=True,
            foreign_key=("ship_mode", "sm_ship_mode_sk"),
        ),
        Column(
            "cs_warehouse_sk",
            DataType.INTEGER,
            nullable=True,
            foreign_key=("warehouse", "w_warehouse_sk"),
        ),
        Column(
            "cs_item_sk",
            DataType.INTEGER,
            nullable=False,
            foreign_key=("item", "i_item_sk"),
        ),
        Column(
            "cs_promo_sk",
            DataType.INTEGER,
            nullable=True,
            foreign_key=("promotion", "p_promo_sk"),
        ),
        Column("cs_order_number", DataType.INTEGER, primary_key=True),
        Column("cs_quantity", DataType.INTEGER, nullable=True),
        Column("cs_wholesale_cost", DataType.DECIMAL, nullable=True),
        Column("cs_list_price", DataType.DECIMAL, nullable=True),
        Column("cs_sales_price", DataType.DECIMAL, nullable=True),
        Column("cs_ext_discount_amt", DataType.DECIMAL, nullable=True),
        Column("cs_ext_sales_price", DataType.DECIMAL, nullable=True),
        Column("cs_ext_wholesale_cost", DataType.DECIMAL, nullable=True),
        Column("cs_ext_list_price", DataType.DECIMAL, nullable=True),
        Column("cs_ext_tax", DataType.DECIMAL, nullable=True),
        Column("cs_coupon_amt", DataType.DECIMAL, nullable=True),
        Column("cs_ext_ship_cost", DataType.DECIMAL, nullable=True),
        Column("cs_net_paid", DataType.DECIMAL, nullable=True),
        Column("cs_net_paid_inc_tax", DataType.DECIMAL, nullable=True),
        Column("cs_net_paid_inc_ship", DataType.DECIMAL, nullable=True),
        Column("cs_net_paid_inc_ship_tax", DataType.DECIMAL, nullable=True),
        Column("cs_net_profit", DataType.DECIMAL, nullable=True),
    ],
)

CATALOG_RETURNS = Table(
    "catalog_returns",
    [
        Column(
            "cr_returned_date_sk",
            DataType.INTEGER,
            nullable=True,
            foreign_key=("date_dim", "d_date_sk"),
        ),
        Column(
            "cr_returned_time_sk",
            DataType.INTEGER,
            nullable=True,
            foreign_key=("time_dim", "t_time_sk"),
        ),
        Column(
            "cr_item_sk",
            DataType.INTEGER,
            nullable=False,
            foreign_key=("item", "i_item_sk"),
        ),
        Column(
            "cr_refunded_customer_sk",
            DataType.INTEGER,
            nullable=True,
            foreign_key=("customer", "c_customer_sk"),
        ),
        Column(
            "cr_refunded_cdemo_sk",
            DataType.INTEGER,
            nullable=True,
            foreign_key=("customer_demographics", "cd_demo_sk"),
        ),
        Column(
            "cr_refunded_hdemo_sk",
            DataType.INTEGER,
            nullable=True,
            foreign_key=("household_demographics", "hd_demo_sk"),
        ),
        Column(
            "cr_refunded_addr_sk",
            DataType.INTEGER,
            nullable=True,
            foreign_key=("customer_address", "ca_address_sk"),
        ),
        Column(
            "cr_returning_customer_sk",
            DataType.INTEGER,
            nullable=True,
            foreign_key=("customer", "c_customer_sk"),
        ),
        Column(
            "cr_returning_cdemo_sk",
            DataType.INTEGER,
            nullable=True,
            foreign_key=("customer_demographics", "cd_demo_sk"),
        ),
        Column(
            "cr_returning_hdemo_sk",
            DataType.INTEGER,
            nullable=True,
            foreign_key=("household_demographics", "hd_demo_sk"),
        ),
        Column(
            "cr_returning_addr_sk",
            DataType.INTEGER,
            nullable=True,
            foreign_key=("customer_address", "ca_address_sk"),
        ),
        Column(
            "cr_call_center_sk",
            DataType.INTEGER,
            nullable=True,
            foreign_key=("call_center", "cc_call_center_sk"),
        ),
        Column(
            "cr_catalog_page_sk",
            DataType.INTEGER,
            nullable=True,
            foreign_key=("catalog_page", "cp_catalog_page_sk"),
        ),
        Column(
            "cr_ship_mode_sk",
            DataType.INTEGER,
            nullable=True,
            foreign_key=("ship_mode", "sm_ship_mode_sk"),
        ),
        Column(
            "cr_warehouse_sk",
            DataType.INTEGER,
            nullable=True,
            foreign_key=("warehouse", "w_warehouse_sk"),
        ),
        Column(
            "cr_reason_sk",
            DataType.INTEGER,
            nullable=True,
            foreign_key=("reason", "r_reason_sk"),
        ),
        Column(
            "cr_order_number",
            DataType.INTEGER,
            primary_key=True,
            foreign_key=("catalog_sales", "cs_order_number"),
        ),
        Column("cr_return_quantity", DataType.INTEGER, nullable=True),
        Column("cr_return_amount", DataType.DECIMAL, nullable=True),
        Column("cr_return_tax", DataType.DECIMAL, nullable=True),
        Column("cr_return_amt_inc_tax", DataType.DECIMAL, nullable=True),
        Column("cr_fee", DataType.DECIMAL, nullable=True),
        Column("cr_return_ship_cost", DataType.DECIMAL, nullable=True),
        Column("cr_refunded_cash", DataType.DECIMAL, nullable=True),
        Column("cr_reversed_charge", DataType.DECIMAL, nullable=True),
        Column("cr_store_credit", DataType.DECIMAL, nullable=True),
        Column("cr_net_loss", DataType.DECIMAL, nullable=True),
    ],
)

INVENTORY = Table(
    "inventory",
    [
        Column(
            "inv_date_sk",
            DataType.INTEGER,
            nullable=False,
            foreign_key=("date_dim", "d_date_sk"),
        ),
        Column(
            "inv_item_sk",
            DataType.INTEGER,
            nullable=False,
            foreign_key=("item", "i_item_sk"),
        ),
        Column(
            "inv_warehouse_sk",
            DataType.INTEGER,
            nullable=False,
            foreign_key=("warehouse", "w_warehouse_sk"),
        ),
        Column("inv_quantity_on_hand", DataType.INTEGER, nullable=True),
    ],
)

# DBGEN Version (metadata table)
DBGEN_VERSION = Table(
    "dbgen_version",
    [
        Column("dv_version", DataType.VARCHAR, size=16, nullable=True),
        Column("dv_create_date", DataType.VARCHAR, size=10, nullable=True),
        Column("dv_create_time", DataType.VARCHAR, size=10, nullable=True),
        Column("dv_cmdline_args", DataType.VARCHAR, size=200, nullable=True),
    ],
)

# Collection of all tables in the TPC-DS schema
# Ordered to respect foreign key dependencies: referenced tables first
