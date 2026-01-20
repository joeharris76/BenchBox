"""ClickBench schema definitions.

This module defines the schema for the ClickBench benchmark, which consists of
a single flat table representing web analytics data with ~100 columns covering
various aspects of web traffic analysis.

The table schema is based on real-world web analytics data and includes
metrics for user sessions, browser information, referrers, search phrases,
geographical data, and various event attributes.

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

from typing import cast

from benchbox.core.tuning import BenchmarkTunings, TableTuning, TuningColumn

# ClickBench uses a single flat table called 'hits' with web analytics data
HITS_TABLE = {
    "name": "hits",
    "columns": [
        {"name": "WatchID", "type": "BIGINT", "nullable": False},
        {"name": "JavaEnable", "type": "SMALLINT", "nullable": False},
        {"name": "Title", "type": "TEXT", "nullable": False},
        {"name": "GoodEvent", "type": "SMALLINT", "nullable": False},
        {"name": "EventTime", "type": "TIMESTAMP", "nullable": False},
        {"name": "EventDate", "type": "DATE", "nullable": False},
        {"name": "CounterID", "type": "INTEGER", "nullable": False},
        {"name": "ClientIP", "type": "INTEGER", "nullable": False},
        {"name": "RegionID", "type": "INTEGER", "nullable": False},
        {"name": "UserID", "type": "BIGINT", "nullable": False},
        {"name": "CounterClass", "type": "SMALLINT", "nullable": False},
        {"name": "OS", "type": "SMALLINT", "nullable": False},
        {"name": "UserAgent", "type": "SMALLINT", "nullable": False},
        {"name": "URL", "type": "TEXT", "nullable": False},
        {"name": "Referer", "type": "TEXT", "nullable": False},
        {"name": "IsRefresh", "type": "SMALLINT", "nullable": False},
        {"name": "RefererCategoryID", "type": "SMALLINT", "nullable": False},
        {"name": "RefererRegionID", "type": "INTEGER", "nullable": False},
        {"name": "URLCategoryID", "type": "SMALLINT", "nullable": False},
        {"name": "URLRegionID", "type": "INTEGER", "nullable": False},
        {"name": "ResolutionWidth", "type": "SMALLINT", "nullable": False},
        {"name": "ResolutionHeight", "type": "SMALLINT", "nullable": False},
        {"name": "ResolutionDepth", "type": "SMALLINT", "nullable": False},
        {"name": "FlashMajor", "type": "SMALLINT", "nullable": False},
        {"name": "FlashMinor", "type": "SMALLINT", "nullable": False},
        {"name": "FlashMinor2", "type": "VARCHAR(255)", "nullable": False},
        {"name": "NetMajor", "type": "SMALLINT", "nullable": False},
        {"name": "NetMinor", "type": "SMALLINT", "nullable": False},
        {"name": "UserAgentMajor", "type": "SMALLINT", "nullable": False},
        {"name": "UserAgentMinor", "type": "VARCHAR(255)", "nullable": False},
        {"name": "CookieEnable", "type": "SMALLINT", "nullable": False},
        {"name": "JavascriptEnable", "type": "SMALLINT", "nullable": False},
        {"name": "IsMobile", "type": "SMALLINT", "nullable": False},
        {"name": "MobilePhone", "type": "SMALLINT", "nullable": False},
        {"name": "MobilePhoneModel", "type": "VARCHAR(255)", "nullable": False},
        {"name": "Params", "type": "TEXT", "nullable": False},
        {"name": "IPNetworkID", "type": "INTEGER", "nullable": False},
        {"name": "TraficSourceID", "type": "SMALLINT", "nullable": False},
        {"name": "SearchEngineID", "type": "SMALLINT", "nullable": False},
        {"name": "SearchPhrase", "type": "VARCHAR(1024)", "nullable": False},
        {"name": "AdvEngineID", "type": "SMALLINT", "nullable": False},
        {"name": "IsArtifical", "type": "SMALLINT", "nullable": False},
        {"name": "WindowClientWidth", "type": "SMALLINT", "nullable": False},
        {"name": "WindowClientHeight", "type": "SMALLINT", "nullable": False},
        {"name": "ClientTimeZone", "type": "SMALLINT", "nullable": False},
        {"name": "ClientEventTime", "type": "TIMESTAMP", "nullable": False},
        {"name": "SilverlightVersion1", "type": "SMALLINT", "nullable": False},
        {"name": "SilverlightVersion2", "type": "SMALLINT", "nullable": False},
        {"name": "SilverlightVersion3", "type": "INTEGER", "nullable": False},
        {"name": "SilverlightVersion4", "type": "SMALLINT", "nullable": False},
        {"name": "PageCharset", "type": "VARCHAR(255)", "nullable": False},
        {"name": "CodeVersion", "type": "INTEGER", "nullable": False},
        {"name": "IsLink", "type": "SMALLINT", "nullable": False},
        {"name": "IsDownload", "type": "SMALLINT", "nullable": False},
        {"name": "IsNotBounce", "type": "SMALLINT", "nullable": False},
        {"name": "FUniqID", "type": "BIGINT", "nullable": False},
        {"name": "OriginalURL", "type": "TEXT", "nullable": False},
        {"name": "HID", "type": "INTEGER", "nullable": False},
        {"name": "IsOldCounter", "type": "SMALLINT", "nullable": False},
        {"name": "IsEvent", "type": "SMALLINT", "nullable": False},
        {"name": "IsParameter", "type": "SMALLINT", "nullable": False},
        {"name": "DontCountHits", "type": "SMALLINT", "nullable": False},
        {"name": "WithHash", "type": "SMALLINT", "nullable": False},
        {"name": "HitColor", "type": "VARCHAR(1)", "nullable": False},
        {"name": "LocalEventTime", "type": "TIMESTAMP", "nullable": False},
        {"name": "Age", "type": "SMALLINT", "nullable": False},
        {"name": "Sex", "type": "SMALLINT", "nullable": False},
        {"name": "Income", "type": "SMALLINT", "nullable": False},
        {"name": "Interests", "type": "SMALLINT", "nullable": False},
        {"name": "Robotness", "type": "SMALLINT", "nullable": False},
        {"name": "RemoteIP", "type": "INTEGER", "nullable": False},
        {"name": "WindowName", "type": "INTEGER", "nullable": False},
        {"name": "OpenerName", "type": "INTEGER", "nullable": False},
        {"name": "HistoryLength", "type": "SMALLINT", "nullable": False},
        {"name": "BrowserLanguage", "type": "VARCHAR(2)", "nullable": False},
        {"name": "BrowserCountry", "type": "VARCHAR(2)", "nullable": False},
        {"name": "SocialNetwork", "type": "VARCHAR(255)", "nullable": False},
        {"name": "SocialAction", "type": "VARCHAR(255)", "nullable": False},
        {"name": "HTTPError", "type": "SMALLINT", "nullable": False},
        {"name": "SendTiming", "type": "INTEGER", "nullable": False},
        {"name": "DNSTiming", "type": "INTEGER", "nullable": False},
        {"name": "ConnectTiming", "type": "INTEGER", "nullable": False},
        {"name": "ResponseStartTiming", "type": "INTEGER", "nullable": False},
        {"name": "ResponseEndTiming", "type": "INTEGER", "nullable": False},
        {"name": "FetchTiming", "type": "INTEGER", "nullable": False},
        {"name": "SocialSourceNetworkID", "type": "SMALLINT", "nullable": False},
        {"name": "SocialSourcePage", "type": "TEXT", "nullable": False},
        {"name": "ParamPrice", "type": "BIGINT", "nullable": False},
        {"name": "ParamOrderID", "type": "VARCHAR(255)", "nullable": False},
        {"name": "ParamCurrency", "type": "VARCHAR(3)", "nullable": False},
        {"name": "ParamCurrencyID", "type": "SMALLINT", "nullable": False},
        {"name": "OpenstatServiceName", "type": "VARCHAR(255)", "nullable": False},
        {"name": "OpenstatCampaignID", "type": "VARCHAR(255)", "nullable": False},
        {"name": "OpenstatAdID", "type": "VARCHAR(255)", "nullable": False},
        {"name": "OpenstatSourceID", "type": "VARCHAR(255)", "nullable": False},
        {"name": "UTMSource", "type": "VARCHAR(255)", "nullable": False},
        {"name": "UTMMedium", "type": "VARCHAR(255)", "nullable": False},
        {"name": "UTMCampaign", "type": "VARCHAR(255)", "nullable": False},
        {"name": "UTMContent", "type": "VARCHAR(255)", "nullable": False},
        {"name": "UTMTerm", "type": "VARCHAR(255)", "nullable": False},
        {"name": "FromTag", "type": "VARCHAR(255)", "nullable": False},
        {"name": "HasGCLID", "type": "SMALLINT", "nullable": False},
        {"name": "RefererHash", "type": "BIGINT", "nullable": False},
        {"name": "URLHash", "type": "BIGINT", "nullable": False},
        {"name": "CLID", "type": "INTEGER", "nullable": False},
    ],
    "primary_key": ["CounterID", "EventDate", "UserID", "EventTime", "WatchID"],
}


def get_create_table_sql(
    dialect: str = "standard",
    enable_primary_keys: bool = True,
    enable_foreign_keys: bool = True,
) -> str:
    """Generate CREATE TABLE SQL for the ClickBench hits table.

    Args:
        dialect: SQL dialect to use ("standard", "postgres", "mysql", etc.)
        enable_primary_keys: Whether to include primary key constraints
        enable_foreign_keys: Whether to include foreign key constraints

    Returns:
        SQL CREATE TABLE statement
    """
    table = HITS_TABLE
    columns = []

    for col in table["columns"]:
        col_def = f"{cast(str, col['name'])} {cast(str, col['type'])}"
        if not col.get("nullable", True):
            col_def += " NOT NULL"
        columns.append(f"  {col_def}")

    # Add primary key
    if table.get("primary_key") and enable_primary_keys:
        pk_cols = ", ".join(cast(list[str], table["primary_key"]))
        columns.append(f"  PRIMARY KEY ({pk_cols})")

    columns_sql = ",\n".join(columns)

    return f"""CREATE TABLE {table["name"]} (
{columns_sql}
);"""


# Schema as a dictionary for compatibility with other benchmarks
TABLES = {"hits": HITS_TABLE}


def get_tunings() -> BenchmarkTunings:
    """Get the default tuning configurations for ClickBench tables.

    These tunings are for web analytics workloads with focus on
    time-series analysis and high-cardinality filtering common in ClickBench queries.

    Returns:
        BenchmarkTunings containing tuning configurations for ClickBench tables
    """
    tunings = BenchmarkTunings("clickbench")

    # Hits table - single large table for web analytics queries
    hits_tuning = TableTuning(
        table_name="hits",
        partitioning=[TuningColumn("EventDate", "DATE", 1)],
        clustering=[
            TuningColumn("CounterID", "INTEGER", 1),
            TuningColumn("UserID", "BIGINT", 2),
        ],
        sorting=[
            TuningColumn("EventTime", "TIMESTAMP", 1),
            TuningColumn("RegionID", "INTEGER", 2),
        ],
    )
    tunings.add_table_tuning(hits_tuning)

    return tunings
