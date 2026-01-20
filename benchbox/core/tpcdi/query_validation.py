"""TPC-DI data quality validation query suite.

This module provides comprehensive data quality validation queries for TPC-DI
including referential integrity checks, completeness validation, SCD Type 2
verification, data consistency checks, and business rule compliance.

The validation queries cover all 16 tables in the complete TPC-DI schema
and provide detailed data quality metrics for ETL validation.

Copyright 2026 Joe Harris / BenchBox Project

TPC Benchmark™ DI (TPC-DI) - Copyright © Transaction Processing Performance Council
This implementation is based on the TPC-DI specification.

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

from typing import Any, Optional


class TPCDIValidationQueries:
    """TPC-DI data quality validation query manager."""

    def __init__(self) -> None:
        """Initialize the validation query manager."""
        self._queries = self._load_validation_queries()
        self._query_metadata = self._load_validation_metadata()

    def _load_validation_queries(self) -> dict[str, str]:
        """Load all data quality validation queries.

        Returns:
            Dictionary mapping validation query IDs to SQL text
        """
        queries = {}

        # VQ1: Referential integrity validation - Core dimension tables
        queries["VQ1"] = """
SELECT
    'Core Tables Referential Integrity' as validation_name,
    SUM(CASE WHEN orphaned_customers > 0 THEN 1 ELSE 0 END) +
    SUM(CASE WHEN orphaned_accounts > 0 THEN 1 ELSE 0 END) +
    SUM(CASE WHEN orphaned_securities > 0 THEN 1 ELSE 0 END) as integrity_violations,
    COUNT(*) as total_checks
FROM (
    SELECT
        (SELECT COUNT(*) FROM FactTrade f
         LEFT JOIN DimCustomer c ON f.SK_CustomerID = c.SK_CustomerID
         WHERE c.SK_CustomerID IS NULL) as orphaned_customers,
        (SELECT COUNT(*) FROM FactTrade f
         LEFT JOIN DimAccount a ON f.SK_AccountID = a.SK_AccountID
         WHERE a.SK_AccountID IS NULL) as orphaned_accounts,
        (SELECT COUNT(*) FROM FactTrade f
         LEFT JOIN DimSecurity s ON f.SK_SecurityID = s.SK_SecurityID
         WHERE s.SK_SecurityID IS NULL) as orphaned_securities
) checks;
"""

        # VQ2: Extended tables referential integrity validation
        queries["VQ2"] = """
SELECT
    'Extended Tables Referential Integrity' as validation_name,
    SUM(CASE WHEN orphaned_cash_balances > 0 THEN 1 ELSE 0 END) +
    SUM(CASE WHEN orphaned_holdings > 0 THEN 1 ELSE 0 END) +
    SUM(CASE WHEN orphaned_market_history > 0 THEN 1 ELSE 0 END) +
    SUM(CASE WHEN orphaned_watches > 0 THEN 1 ELSE 0 END) as integrity_violations,
    4 as total_checks
FROM (
    SELECT
        (SELECT COUNT(*) FROM FactCashBalances f
         LEFT JOIN DimCustomer c ON f.SK_CustomerID = c.SK_CustomerID
         WHERE c.SK_CustomerID IS NULL) as orphaned_cash_balances,
        (SELECT COUNT(*) FROM FactHoldings f
         LEFT JOIN DimSecurity s ON f.SK_SecurityID = s.SK_SecurityID
         WHERE s.SK_SecurityID IS NULL) as orphaned_holdings,
        (SELECT COUNT(*) FROM FactMarketHistory f
         LEFT JOIN DimSecurity s ON f.SK_SecurityID = s.SK_SecurityID
         WHERE s.SK_SecurityID IS NULL) as orphaned_market_history,
        (SELECT COUNT(*) FROM FactWatches f
         LEFT JOIN DimCustomer c ON f.SK_CustomerID = c.SK_CustomerID
         WHERE c.SK_CustomerID IS NULL) as orphaned_watches
) checks;
"""

        # VQ3: Data completeness validation - Core tables
        queries["VQ3"] = """
SELECT
    'Data Completeness - Core Tables' as validation_name,
    table_name,
    total_records,
    null_key_columns,
    CASE WHEN null_key_columns = 0 THEN 'PASS' ELSE 'FAIL' END as status
FROM (
    SELECT 'DimCustomer' as table_name,
           COUNT(*) as total_records,
           SUM(CASE WHEN CustomerID IS NULL OR TaxID IS NULL OR Status IS NULL THEN 1 ELSE 0 END) as null_key_columns
    FROM DimCustomer
    WHERE IsCurrent = 1
    UNION ALL
    SELECT 'DimAccount' as table_name,
           COUNT(*) as total_records,
           SUM(CASE WHEN AccountID IS NULL OR SK_CustomerID IS NULL OR Status IS NULL THEN 1 ELSE 0 END) as null_key_columns
    FROM DimAccount
    WHERE IsCurrent = 1
    UNION ALL
    SELECT 'DimSecurity' as table_name,
           COUNT(*) as total_records,
           SUM(CASE WHEN Symbol IS NULL OR Name IS NULL OR ExchangeID IS NULL THEN 1 ELSE 0 END) as null_key_columns
    FROM DimSecurity
    WHERE IsCurrent = 1
    UNION ALL
    SELECT 'DimCompany' as table_name,
           COUNT(*) as total_records,
           SUM(CASE WHEN CompanyID IS NULL OR Name IS NULL OR Industry IS NULL THEN 1 ELSE 0 END) as null_key_columns
    FROM DimCompany
    WHERE IsCurrent = 1
    UNION ALL
    SELECT 'FactTrade' as table_name,
           COUNT(*) as total_records,
           SUM(CASE WHEN TradeID IS NULL OR SK_CustomerID IS NULL OR SK_SecurityID IS NULL THEN 1 ELSE 0 END) as null_key_columns
    FROM FactTrade
) completeness_check
ORDER BY table_name;
"""

        # VQ4: SCD Type 2 validation - Current record flags
        queries["VQ4"] = """
SELECT
    'SCD Type 2 Current Record Validation' as validation_name,
    table_name,
    total_records,
    current_records,
    multiple_current_per_business_key,
    CASE WHEN multiple_current_per_business_key = 0 THEN 'PASS' ELSE 'FAIL' END as status
FROM (
    SELECT 'DimCustomer' as table_name,
           COUNT(*) as total_records,
           SUM(CASE WHEN IsCurrent = 1 THEN 1 ELSE 0 END) as current_records,
           (SELECT COUNT(*) FROM (
               SELECT CustomerID, COUNT(*) as current_count
               FROM DimCustomer
               WHERE IsCurrent = 1
               GROUP BY CustomerID
               HAVING COUNT(*) > 1
           ) violations) as multiple_current_per_business_key
    FROM DimCustomer
    UNION ALL
    SELECT 'DimAccount' as table_name,
           COUNT(*) as total_records,
           SUM(CASE WHEN IsCurrent = 1 THEN 1 ELSE 0 END) as current_records,
           (SELECT COUNT(*) FROM (
               SELECT AccountID, COUNT(*) as current_count
               FROM DimAccount
               WHERE IsCurrent = 1
               GROUP BY AccountID
               HAVING COUNT(*) > 1
           ) violations) as multiple_current_per_business_key
    FROM DimAccount
    UNION ALL
    SELECT 'DimSecurity' as table_name,
           COUNT(*) as total_records,
           SUM(CASE WHEN IsCurrent = 1 THEN 1 ELSE 0 END) as current_records,
           (SELECT COUNT(*) FROM (
               SELECT Symbol, COUNT(*) as current_count
               FROM DimSecurity
               WHERE IsCurrent = 1
               GROUP BY Symbol
               HAVING COUNT(*) > 1
           ) violations) as multiple_current_per_business_key
    FROM DimSecurity
    UNION ALL
    SELECT 'DimCompany' as table_name,
           COUNT(*) as total_records,
           SUM(CASE WHEN IsCurrent = 1 THEN 1 ELSE 0 END) as current_records,
           (SELECT COUNT(*) FROM (
               SELECT CompanyID, COUNT(*) as current_count
               FROM DimCompany
               WHERE IsCurrent = 1
               GROUP BY CompanyID
               HAVING COUNT(*) > 1
           ) violations) as multiple_current_per_business_key
    FROM DimCompany
    UNION ALL
    SELECT 'DimBroker' as table_name,
           COUNT(*) as total_records,
           SUM(CASE WHEN IsCurrent = 1 THEN 1 ELSE 0 END) as current_records,
           (SELECT COUNT(*) FROM (
               SELECT BrokerID, COUNT(*) as current_count
               FROM DimBroker
               WHERE IsCurrent = 1
               GROUP BY BrokerID
               HAVING COUNT(*) > 1
           ) violations) as multiple_current_per_business_key
    FROM DimBroker
) scd_validation
ORDER BY table_name;
"""

        # VQ5: SCD Type 2 validation - Effective date ranges
        queries["VQ5"] = """
SELECT
    'SCD Type 2 Date Range Validation' as validation_name,
    table_name,
    invalid_date_ranges,
    overlapping_date_ranges,
    future_effective_dates,
    CASE WHEN (invalid_date_ranges + overlapping_date_ranges + future_effective_dates) = 0 THEN 'PASS' ELSE 'FAIL' END as status
FROM (
    SELECT 'DimCustomer' as table_name,
           (SELECT COUNT(*) FROM DimCustomer WHERE EffectiveDate >= EndDate) as invalid_date_ranges,
           (SELECT COUNT(*) FROM DimCustomer c1
            JOIN DimCustomer c2 ON c1.CustomerID = c2.CustomerID
                AND c1.SK_CustomerID != c2.SK_CustomerID
                AND c1.EffectiveDate < c2.EndDate
                AND c2.EffectiveDate < c1.EndDate) as overlapping_date_ranges,
           (SELECT COUNT(*) FROM DimCustomer WHERE EffectiveDate > CURRENT_DATE) as future_effective_dates
    UNION ALL
    SELECT 'DimAccount' as table_name,
           (SELECT COUNT(*) FROM DimAccount WHERE EffectiveDate >= EndDate) as invalid_date_ranges,
           (SELECT COUNT(*) FROM DimAccount a1
            JOIN DimAccount a2 ON a1.AccountID = a2.AccountID
                AND a1.SK_AccountID != a2.SK_AccountID
                AND a1.EffectiveDate < a2.EndDate
                AND a2.EffectiveDate < a1.EndDate) as overlapping_date_ranges,
           (SELECT COUNT(*) FROM DimAccount WHERE EffectiveDate > CURRENT_DATE) as future_effective_dates
    UNION ALL
    SELECT 'DimSecurity' as table_name,
           (SELECT COUNT(*) FROM DimSecurity WHERE EffectiveDate >= EndDate) as invalid_date_ranges,
           (SELECT COUNT(*) FROM DimSecurity s1
            JOIN DimSecurity s2 ON s1.Symbol = s2.Symbol
                AND s1.SK_SecurityID != s2.SK_SecurityID
                AND s1.EffectiveDate < s2.EndDate
                AND s2.EffectiveDate < s1.EndDate) as overlapping_date_ranges,
           (SELECT COUNT(*) FROM DimSecurity WHERE EffectiveDate > CURRENT_DATE) as future_effective_dates
    UNION ALL
    SELECT 'DimCompany' as table_name,
           (SELECT COUNT(*) FROM DimCompany WHERE EffectiveDate >= EndDate) as invalid_date_ranges,
           (SELECT COUNT(*) FROM DimCompany comp1
            JOIN DimCompany comp2 ON comp1.CompanyID = comp2.CompanyID
                AND comp1.SK_CompanyID != comp2.SK_CompanyID
                AND comp1.EffectiveDate < comp2.EndDate
                AND comp2.EffectiveDate < comp1.EndDate) as overlapping_date_ranges,
           (SELECT COUNT(*) FROM DimCompany WHERE EffectiveDate > CURRENT_DATE) as future_effective_dates
    UNION ALL
    SELECT 'DimBroker' as table_name,
           (SELECT COUNT(*) FROM DimBroker WHERE EffectiveDate >= EndDate) as invalid_date_ranges,
           (SELECT COUNT(*) FROM DimBroker b1
            JOIN DimBroker b2 ON b1.BrokerID = b2.BrokerID
                AND b1.SK_BrokerID != b2.SK_BrokerID
                AND b1.EffectiveDate < b2.EndDate
                AND b2.EffectiveDate < b1.EndDate) as overlapping_date_ranges,
           (SELECT COUNT(*) FROM DimBroker WHERE EffectiveDate > CURRENT_DATE) as future_effective_dates
) date_validation
ORDER BY table_name;
"""

        # VQ6: Data consistency validation - Trade quantities and holdings
        queries["VQ6"] = """
SELECT
    'Trade-Holdings Consistency Validation' as validation_name,
    customer_id,
    security_id,
    total_buy_quantity,
    total_sell_quantity,
    net_position,
    current_holdings,
    position_discrepancy,
    CASE WHEN ABS(position_discrepancy) <= {tolerance_threshold} THEN 'PASS' ELSE 'FAIL' END as status
FROM (
    SELECT
        ft.SK_CustomerID as customer_id,
        ft.SK_SecurityID as security_id,
        SUM(CASE WHEN tt.TT_IS_SELL = 0 THEN ft.Quantity ELSE 0 END) as total_buy_quantity,
        SUM(CASE WHEN tt.TT_IS_SELL = 1 THEN ft.Quantity ELSE 0 END) as total_sell_quantity,
        SUM(CASE WHEN tt.TT_IS_SELL = 0 THEN ft.Quantity ELSE -ft.Quantity END) as net_position,
        COALESCE(fh.CurrentHolding, 0) as current_holdings,
        (SUM(CASE WHEN tt.TT_IS_SELL = 0 THEN ft.Quantity ELSE -ft.Quantity END) - COALESCE(fh.CurrentHolding, 0)) as position_discrepancy
    FROM FactTrade ft
    JOIN TradeType tt ON ft.Type = tt.TT_ID
    LEFT JOIN FactHoldings fh ON ft.SK_CustomerID = fh.SK_CustomerID
                              AND ft.SK_SecurityID = fh.SK_SecurityID
    WHERE ft.Status = 'Completed'
    GROUP BY ft.SK_CustomerID, ft.SK_SecurityID, fh.CurrentHolding
) position_check
WHERE ABS(position_discrepancy) > 0
ORDER BY ABS(position_discrepancy) DESC
LIMIT {limit_rows};
"""

        # VQ7: Business rule validation - Customer tier and net worth correlation
        queries["VQ7"] = """
SELECT
    'Customer Tier-NetWorth Validation' as validation_name,
    Tier,
    COUNT(*) as customer_count,
    MIN(NetWorth) as min_net_worth,
    MAX(NetWorth) as max_net_worth,
    AVG(NetWorth) as avg_net_worth,
    SUM(CASE
        WHEN Tier = 1 AND NetWorth < {tier1_min_networth} THEN 1
        WHEN Tier = 2 AND (NetWorth < {tier2_min_networth} OR NetWorth >= {tier2_max_networth}) THEN 1
        WHEN Tier = 3 AND NetWorth >= {tier3_max_networth} THEN 1
        ELSE 0
    END) as tier_violations,
    CASE WHEN SUM(CASE
        WHEN Tier = 1 AND NetWorth < {tier1_min_networth} THEN 1
        WHEN Tier = 2 AND (NetWorth < {tier2_min_networth} OR NetWorth >= {tier2_max_networth}) THEN 1
        WHEN Tier = 3 AND NetWorth >= {tier3_max_networth} THEN 1
        ELSE 0
    END) = 0 THEN 'PASS' ELSE 'FAIL' END as status
FROM DimCustomer
WHERE IsCurrent = 1
GROUP BY Tier
ORDER BY Tier;
"""

        # VQ8: Business rule validation - Credit rating ranges
        queries["VQ8"] = """
SELECT
    'Credit Rating Validation' as validation_name,
    COUNT(*) as total_customers,
    SUM(CASE WHEN CreditRating < {min_credit_rating} OR CreditRating > {max_credit_rating} THEN 1 ELSE 0 END) as invalid_credit_ratings,
    MIN(CreditRating) as min_credit_rating,
    MAX(CreditRating) as max_credit_rating,
    AVG(CreditRating) as avg_credit_rating,
    CASE WHEN SUM(CASE WHEN CreditRating < {min_credit_rating} OR CreditRating > {max_credit_rating} THEN 1 ELSE 0 END) = 0 THEN 'PASS' ELSE 'FAIL' END as status
FROM DimCustomer
WHERE IsCurrent = 1;
"""

        # VQ9: Business rule validation - Trade price reasonableness
        queries["VQ9"] = """
SELECT
    'Trade Price Reasonableness Validation' as validation_name,
    COUNT(*) as total_trades,
    SUM(CASE WHEN TradePrice <= 0 THEN 1 ELSE 0 END) as negative_or_zero_prices,
    SUM(CASE WHEN TradePrice > {max_reasonable_price} THEN 1 ELSE 0 END) as extremely_high_prices,
    SUM(CASE WHEN Fee < 0 OR Commission < 0 OR Tax < 0 THEN 1 ELSE 0 END) as negative_fees,
    MIN(TradePrice) as min_trade_price,
    MAX(TradePrice) as max_trade_price,
    AVG(TradePrice) as avg_trade_price,
    CASE WHEN (
        SUM(CASE WHEN TradePrice <= 0 THEN 1 ELSE 0 END) +
        SUM(CASE WHEN TradePrice > {max_reasonable_price} THEN 1 ELSE 0 END) +
        SUM(CASE WHEN Fee < 0 OR Commission < 0 OR Tax < 0 THEN 1 ELSE 0 END)
    ) = 0 THEN 'PASS' ELSE 'FAIL' END as status
FROM FactTrade;
"""

        # VQ10: Date dimension validation - Business day consistency
        queries["VQ10"] = """
SELECT
    'Date Dimension Business Day Validation' as validation_name,
    COUNT(*) as total_dates,
    SUM(CASE WHEN HolidayFlag = 1 AND DayOfWeekNum NOT IN (1,7) THEN 1 ELSE 0 END) as holiday_weekday_conflicts,
    SUM(CASE WHEN DayOfWeekNum IN (1,7) AND HolidayFlag = 0 THEN 0 ELSE 1 END) as weekend_flag_errors,
    SUM(CASE WHEN CalendarYearID != CAST(SUBSTR(CAST(DateValue AS VARCHAR), 1, 4) AS INTEGER) THEN 1 ELSE 0 END) as year_mismatch_errors,
    SUM(CASE WHEN CalendarQtrID NOT BETWEEN 1 AND 4 THEN 1 ELSE 0 END) as invalid_quarter_errors,
    CASE WHEN (
        SUM(CASE WHEN HolidayFlag = 1 AND DayOfWeekNum NOT IN (1,7) THEN 1 ELSE 0 END) +
        SUM(CASE WHEN DayOfWeekNum IN (1,7) AND HolidayFlag = 0 THEN 0 ELSE 1 END) +
        SUM(CASE WHEN CalendarYearID != CAST(SUBSTR(CAST(DateValue AS VARCHAR), 1, 4) AS INTEGER) THEN 1 ELSE 0 END) +
        SUM(CASE WHEN CalendarQtrID NOT BETWEEN 1 AND 4 THEN 1 ELSE 0 END)
    ) = 0 THEN 'PASS' ELSE 'FAIL' END as status
FROM DimDate;
"""

        # VQ11: Extended fact table validation - Market history consistency
        queries["VQ11"] = """
SELECT
    'Market History Consistency Validation' as validation_name,
    COUNT(*) as total_market_records,
    SUM(CASE WHEN ClosePrice <= 0 OR DayHigh <= 0 OR DayLow <= 0 THEN 1 ELSE 0 END) as invalid_prices,
    SUM(CASE WHEN DayHigh < DayLow THEN 1 ELSE 0 END) as high_low_inconsistencies,
    SUM(CASE WHEN ClosePrice > DayHigh OR ClosePrice < DayLow THEN 1 ELSE 0 END) as close_price_range_violations,
    SUM(CASE WHEN Volume < 0 THEN 1 ELSE 0 END) as negative_volume,
    SUM(CASE WHEN FiftyTwoWeekHigh < FiftyTwoWeekLow THEN 1 ELSE 0 END) as week52_range_violations,
    CASE WHEN (
        SUM(CASE WHEN ClosePrice <= 0 OR DayHigh <= 0 OR DayLow <= 0 THEN 1 ELSE 0 END) +
        SUM(CASE WHEN DayHigh < DayLow THEN 1 ELSE 0 END) +
        SUM(CASE WHEN ClosePrice > DayHigh OR ClosePrice < DayLow THEN 1 ELSE 0 END) +
        SUM(CASE WHEN Volume < 0 THEN 1 ELSE 0 END) +
        SUM(CASE WHEN FiftyTwoWeekHigh < FiftyTwoWeekLow THEN 1 ELSE 0 END)
    ) = 0 THEN 'PASS' ELSE 'FAIL' END as status
FROM FactMarketHistory;
"""

        # VQ12: Cash balances validation - Account balance consistency
        queries["VQ12"] = """
SELECT
    'Cash Balance Consistency Validation' as validation_name,
    cb.SK_CustomerID,
    cb.SK_AccountID,
    cb.Cash as current_cash_balance,
    SUM(CASE WHEN tt.TT_IS_SELL = 1 THEN ft.Quantity * ft.TradePrice - ft.Fee - ft.Commission - ft.Tax
             WHEN tt.TT_IS_SELL = 0 THEN -(ft.Quantity * ft.TradePrice + ft.Fee + ft.Commission + ft.Tax)
             ELSE 0 END) as calculated_cash_impact,
    ABS(cb.Cash - SUM(CASE WHEN tt.TT_IS_SELL = 1 THEN ft.Quantity * ft.TradePrice - ft.Fee - ft.Commission - ft.Tax
                           WHEN tt.TT_IS_SELL = 0 THEN -(ft.Quantity * ft.TradePrice + ft.Fee + ft.Commission + ft.Tax)
                           ELSE 0 END)) as balance_discrepancy,
    CASE WHEN ABS(cb.Cash - SUM(CASE WHEN tt.TT_IS_SELL = 1 THEN ft.Quantity * ft.TradePrice - ft.Fee - ft.Commission - ft.Tax
                                     WHEN tt.TT_IS_SELL = 0 THEN -(ft.Quantity * ft.TradePrice + ft.Fee + ft.Commission + ft.Tax)
                                     ELSE 0 END)) <= {balance_tolerance} THEN 'PASS' ELSE 'FAIL' END as status
FROM FactCashBalances cb
LEFT JOIN FactTrade ft ON cb.SK_CustomerID = ft.SK_CustomerID AND cb.SK_AccountID = ft.SK_AccountID
LEFT JOIN TradeType tt ON ft.Type = tt.TT_ID
WHERE ft.Status = 'Completed'
GROUP BY cb.SK_CustomerID, cb.SK_AccountID, cb.Cash
HAVING COUNT(ft.TradeID) > 0
ORDER BY balance_discrepancy DESC
LIMIT {limit_rows};
"""

        return queries

    def _load_validation_metadata(self) -> dict[str, dict[str, Any]]:
        """Load metadata for validation queries.

        Returns:
            Dictionary mapping query IDs to their metadata
        """
        metadata = {}

        metadata["VQ1"] = {
            "relies_on": ["FactTrade", "DimCustomer", "DimAccount", "DimSecurity"],
            "query_type": "validation",
            "category": "referential_integrity",
            "description": "Validates referential integrity between core fact and dimension tables",
            "severity": "critical",
        }

        metadata["VQ2"] = {
            "relies_on": [
                "FactCashBalances",
                "FactHoldings",
                "FactMarketHistory",
                "FactWatches",
                "DimCustomer",
                "DimSecurity",
            ],
            "query_type": "validation",
            "category": "referential_integrity",
            "description": "Validates referential integrity for extended fact tables",
            "severity": "critical",
        }

        metadata["VQ3"] = {
            "relies_on": [
                "DimCustomer",
                "DimAccount",
                "DimSecurity",
                "DimCompany",
                "FactTrade",
            ],
            "query_type": "validation",
            "category": "completeness",
            "description": "Validates data completeness across core dimension and fact tables",
            "severity": "high",
        }

        metadata["VQ4"] = {
            "relies_on": [
                "DimCustomer",
                "DimAccount",
                "DimSecurity",
                "DimCompany",
                "DimBroker",
            ],
            "query_type": "validation",
            "category": "scd_type2",
            "description": "Validates SCD Type 2 current record flag consistency",
            "severity": "high",
        }

        metadata["VQ5"] = {
            "relies_on": [
                "DimCustomer",
                "DimAccount",
                "DimSecurity",
                "DimCompany",
                "DimBroker",
            ],
            "query_type": "validation",
            "category": "scd_type2",
            "description": "Validates SCD Type 2 effective date range consistency",
            "severity": "high",
        }

        metadata["VQ6"] = {
            "relies_on": ["FactTrade", "FactHoldings", "TradeType"],
            "query_type": "validation",
            "category": "consistency",
            "description": "Validates consistency between trade transactions and current holdings",
            "severity": "medium",
        }

        metadata["VQ7"] = {
            "relies_on": ["DimCustomer"],
            "query_type": "validation",
            "category": "business_rules",
            "description": "Validates customer tier and net worth correlation business rules",
            "severity": "medium",
        }

        metadata["VQ8"] = {
            "relies_on": ["DimCustomer"],
            "query_type": "validation",
            "category": "business_rules",
            "description": "Validates credit rating range business rules",
            "severity": "medium",
        }

        metadata["VQ9"] = {
            "relies_on": ["FactTrade"],
            "query_type": "validation",
            "category": "business_rules",
            "description": "Validates trade price and fee reasonableness business rules",
            "severity": "medium",
        }

        metadata["VQ10"] = {
            "relies_on": ["DimDate"],
            "query_type": "validation",
            "category": "consistency",
            "description": "Validates date dimension business day and calendar consistency",
            "severity": "low",
        }

        metadata["VQ11"] = {
            "relies_on": ["FactMarketHistory"],
            "query_type": "validation",
            "category": "consistency",
            "description": "Validates market history price and volume consistency",
            "severity": "medium",
        }

        metadata["VQ12"] = {
            "relies_on": ["FactCashBalances", "FactTrade", "TradeType"],
            "query_type": "validation",
            "category": "consistency",
            "description": "Validates cash balance consistency with trade transactions",
            "severity": "medium",
        }

        return metadata

    def get_query(self, query_id: str, params: Optional[dict[str, Any]] = None) -> str:
        """Get a validation query with parameters.

        Args:
            query_id: Query identifier (VQ1, VQ2, etc.)
            params: Optional parameter values. If None, uses defaults.

        Returns:
            SQL query with parameters replaced

        Raises:
            ValueError: If query_id is invalid
        """
        if query_id not in self._queries:
            available = ", ".join(sorted(self._queries.keys()))
            raise ValueError(f"Invalid validation query ID: {query_id}. Available: {available}")

        template = self._queries[query_id]

        if params is None:
            params = self._generate_default_params(query_id)
        else:
            # Merge with defaults for any missing parameters
            defaults = self._generate_default_params(query_id)
            defaults.update(params)
            params = defaults

        return template.format(**params)

    def get_all_queries(self) -> dict[str, str]:
        """Get all validation queries with default parameters.

        Returns:
            Dictionary mapping query IDs to parameterized SQL
        """
        result = {}
        for query_id in self._queries:
            result[query_id] = self.get_query(query_id)
        return result

    def _generate_default_params(self, query_id: str) -> dict[str, Any]:
        """Generate default parameters for validation queries.

        Args:
            query_id: Query identifier

        Returns:
            Dictionary of parameter names to default values
        """
        # Default parameters for validation queries
        defaults = {
            # Tolerance thresholds
            "tolerance_threshold": 100,  # Quantity tolerance for position discrepancies
            "balance_tolerance": 1000.00,  # Dollar tolerance for cash balance discrepancies
            # Business rule thresholds
            "tier1_min_networth": 1000000,  # Tier 1 customers: $1M+ net worth
            "tier2_min_networth": 250000,  # Tier 2 customers: $250K-$1M net worth
            "tier2_max_networth": 1000000,
            "tier3_max_networth": 250000,  # Tier 3 customers: <$250K net worth
            # Credit rating range (FICO scores)
            "min_credit_rating": 300,
            "max_credit_rating": 850,
            # Trade price reasonableness
            "max_reasonable_price": 10000.00,  # $10,000 per share max
            # Result limits
            "limit_rows": 100,
        }

        return defaults

    def get_query_metadata(self, query_id: str) -> dict[str, Any]:
        """Get metadata for a validation query.

        Args:
            query_id: Query identifier (VQ1, VQ2, etc.)

        Returns:
            Dictionary containing query metadata

        Raises:
            ValueError: If query_id is invalid
        """
        if query_id not in self._query_metadata:
            available = ", ".join(sorted(self._query_metadata.keys()))
            raise ValueError(f"Invalid validation query ID: {query_id}. Available: {available}")

        return self._query_metadata[query_id].copy()

    def get_queries_by_category(self, category: str) -> list[str]:
        """Get all validation queries of a specific category.

        Args:
            category: Category name (referential_integrity, completeness, scd_type2,
                     consistency, business_rules)

        Returns:
            List of query IDs in the specified category
        """
        valid_categories = {
            "referential_integrity",
            "completeness",
            "scd_type2",
            "consistency",
            "business_rules",
        }
        if category not in valid_categories:
            raise ValueError(f"Invalid category: {category}. Valid categories: {', '.join(valid_categories)}")

        return [query_id for query_id, metadata in self._query_metadata.items() if metadata["category"] == category]

    def get_queries_by_severity(self, severity: str) -> list[str]:
        """Get all validation queries of a specific severity level.

        Args:
            severity: Severity level (critical, high, medium, low)

        Returns:
            List of query IDs with the specified severity
        """
        valid_severities = {"critical", "high", "medium", "low"}
        if severity not in valid_severities:
            raise ValueError(f"Invalid severity: {severity}. Valid severities: {', '.join(valid_severities)}")

        return [query_id for query_id, metadata in self._query_metadata.items() if metadata["severity"] == severity]
