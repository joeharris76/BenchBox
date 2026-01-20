"""TPC-DI business intelligence analytical query suite.

This module provides comprehensive analytical queries for TPC-DI covering
customer profitability analysis, security performance analysis, broker
performance metrics, market trend analysis, and portfolio analysis.

These queries demonstrate realistic business intelligence workloads on the
complete TPC-DI data warehouse schema with 16 tables.

Copyright 2026 Joe Harris / BenchBox Project

TPC Benchmark™ DI (TPC-DI) - Copyright © Transaction Processing Performance Council
This implementation is based on the TPC-DI specification.

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

from typing import Any, Optional


class TPCDIAnalyticalQueries:
    """TPC-DI business intelligence analytical query manager."""

    def __init__(self) -> None:
        """Initialize the analytical query manager."""
        self._queries = self._load_analytical_queries()
        self._query_metadata = self._load_analytical_metadata()

    def _load_analytical_queries(self) -> dict[str, str]:
        """Load all business intelligence analytical queries.

        Returns:
            Dictionary mapping analytical query IDs to SQL text
        """
        queries = {}

        # AQ1: Customer profitability analysis by tier and demographics
        queries["AQ1"] = """
SELECT
    'Customer Profitability Analysis' as analysis_name,
    c.Tier,
    c.Country,
    c.Gender,
    COUNT(DISTINCT c.SK_CustomerID) as customer_count,
    COUNT(t.TradeID) as total_trades,
    SUM(t.Quantity * t.TradePrice) as total_trade_value,
    SUM(t.Fee + t.Commission + t.Tax) as total_fees_generated,
    AVG(t.TradePrice) as avg_trade_price,
    AVG(c.NetWorth) as avg_net_worth,
    AVG(c.CreditRating) as avg_credit_rating,
    SUM(t.Quantity * t.TradePrice) / COUNT(DISTINCT c.SK_CustomerID) as revenue_per_customer,
    SUM(t.Fee + t.Commission + t.Tax) / COUNT(DISTINCT c.SK_CustomerID) as fees_per_customer,
    COUNT(t.TradeID) / COUNT(DISTINCT c.SK_CustomerID) as trades_per_customer
FROM DimCustomer c
LEFT JOIN FactTrade t ON c.SK_CustomerID = t.SK_CustomerID
LEFT JOIN DimDate d ON t.SK_CreateDateID = d.SK_DateID
WHERE c.IsCurrent = 1
  AND (d.CalendarYearID >= {start_year} AND d.CalendarYearID <= {end_year})
  AND t.Status = 'Completed'
GROUP BY c.Tier, c.Country, c.Gender
HAVING COUNT(t.TradeID) > {min_trades}
ORDER BY total_fees_generated DESC, total_trade_value DESC
LIMIT {limit_rows};
"""

        # AQ2: Security performance analysis with price movements and volatility
        queries["AQ2"] = """
SELECT
    'Security Performance Analysis' as analysis_name,
    s.Symbol,
    s.Name as security_name,
    comp.Name as company_name,
    comp.Industry,
    comp.SPrating,
    COUNT(t.TradeID) as total_trades,
    SUM(t.Quantity) as total_volume,
    AVG(t.TradePrice) as avg_trade_price,
    MIN(t.TradePrice) as min_trade_price,
    MAX(t.TradePrice) as max_trade_price,
    (MAX(t.TradePrice) - MIN(t.TradePrice)) / MIN(t.TradePrice) * 100 as price_volatility_pct,
    SUM(t.Quantity * t.TradePrice) as total_market_value,
    AVG(mh.ClosePrice) as avg_market_close,
    AVG(mh.Volume) as avg_daily_volume,
    AVG(mh.PERatio) as avg_pe_ratio,
    AVG(mh.Yield) as avg_dividend_yield,
    MAX(mh.FiftyTwoWeekHigh) as week52_high,
    MIN(mh.FiftyTwoWeekLow) as week52_low
FROM DimSecurity s
JOIN DimCompany comp ON s.SK_CompanyID = comp.SK_CompanyID
LEFT JOIN FactTrade t ON s.SK_SecurityID = t.SK_SecurityID
LEFT JOIN FactMarketHistory mh ON s.SK_SecurityID = mh.SK_SecurityID
LEFT JOIN DimDate d ON t.SK_CreateDateID = d.SK_DateID
WHERE s.IsCurrent = 1
  AND comp.IsCurrent = 1
  AND (d.CalendarYearID >= {start_year} AND d.CalendarYearID <= {end_year})
  AND t.Status = 'Completed'
GROUP BY s.Symbol, s.Name, comp.Name, comp.Industry, comp.SPrating
HAVING COUNT(t.TradeID) > {min_trades}
ORDER BY total_market_value DESC, price_volatility_pct DESC
LIMIT {limit_rows};
"""

        # AQ3: Broker performance analysis with commission and trade volume metrics
        queries["AQ3"] = """
SELECT
    'Broker Performance Analysis' as analysis_name,
    b.FirstName || ' ' || b.LastName as broker_name,
    b.Branch,
    b.Office,
    COUNT(DISTINCT t.SK_CustomerID) as unique_customers,
    COUNT(t.TradeID) as total_trades,
    SUM(t.Quantity) as total_quantity_traded,
    SUM(t.Quantity * t.TradePrice) as total_trade_value,
    SUM(t.Commission) as total_commission_generated,
    AVG(t.Commission) as avg_commission_per_trade,
    AVG(t.TradePrice) as avg_trade_price,
    SUM(t.Commission) / SUM(t.Quantity * t.TradePrice) * 100 as commission_rate_pct,
    COUNT(t.TradeID) / COUNT(DISTINCT t.SK_CustomerID) as trades_per_customer,
    SUM(t.Quantity * t.TradePrice) / COUNT(DISTINCT t.SK_CustomerID) as value_per_customer,
    COUNT(CASE WHEN tt.TT_IS_SELL = 1 THEN 1 END) as sell_trades,
    COUNT(CASE WHEN tt.TT_IS_SELL = 0 THEN 1 END) as buy_trades,
    COUNT(CASE WHEN tt.TT_IS_SELL = 1 THEN 1 END) / COUNT(t.TradeID) * 100 as sell_ratio_pct
FROM DimBroker b
LEFT JOIN FactTrade t ON b.SK_BrokerID = t.SK_BrokerID
LEFT JOIN TradeType tt ON t.Type = tt.TT_ID
LEFT JOIN DimDate d ON t.SK_CreateDateID = d.SK_DateID
WHERE b.IsCurrent = 1
  AND (d.CalendarYearID >= {start_year} AND d.CalendarYearID <= {end_year})
  AND t.Status = 'Completed'
GROUP BY b.FirstName, b.LastName, b.Branch, b.Office
HAVING COUNT(t.TradeID) > {min_trades}
ORDER BY total_commission_generated DESC, total_trade_value DESC
LIMIT {limit_rows};
"""

        # AQ4: Market trend analysis with time-series aggregations
        queries["AQ4"] = """
SELECT
    'Market Trend Analysis' as analysis_name,
    d.CalendarYearID,
    d.CalendarQtrID,
    d.CalendarMonthID,
    COUNT(t.TradeID) as monthly_trade_count,
    COUNT(DISTINCT t.SK_CustomerID) as active_customers,
    COUNT(DISTINCT t.SK_SecurityID) as securities_traded,
    SUM(t.Quantity) as monthly_volume,
    SUM(t.Quantity * t.TradePrice) as monthly_trade_value,
    AVG(t.TradePrice) as avg_monthly_price,
    SUM(t.Fee + t.Commission + t.Tax) as monthly_fees,
    AVG(mh.ClosePrice) as avg_market_close,
    AVG(mh.Volume) as avg_market_volume,
    SUM(CASE WHEN tt.TT_IS_SELL = 1 THEN t.Quantity * t.TradePrice ELSE 0 END) as sell_volume,
    SUM(CASE WHEN tt.TT_IS_SELL = 0 THEN t.Quantity * t.TradePrice ELSE 0 END) as buy_volume,
    (SUM(CASE WHEN tt.TT_IS_SELL = 1 THEN t.Quantity * t.TradePrice ELSE 0 END) -
     SUM(CASE WHEN tt.TT_IS_SELL = 0 THEN t.Quantity * t.TradePrice ELSE 0 END)) as net_market_flow,
    LAG(SUM(t.Quantity * t.TradePrice)) OVER (ORDER BY d.CalendarYearID, d.CalendarMonthID) as prev_month_value,
    (SUM(t.Quantity * t.TradePrice) - LAG(SUM(t.Quantity * t.TradePrice)) OVER (ORDER BY d.CalendarYearID, d.CalendarMonthID)) /
     LAG(SUM(t.Quantity * t.TradePrice)) OVER (ORDER BY d.CalendarYearID, d.CalendarMonthID) * 100 as month_over_month_growth_pct
FROM DimDate d
LEFT JOIN FactTrade t ON d.SK_DateID = t.SK_CreateDateID
LEFT JOIN FactMarketHistory mh ON d.SK_DateID = mh.SK_DateID
LEFT JOIN TradeType tt ON t.Type = tt.TT_ID
WHERE d.CalendarYearID >= {start_year}
  AND d.CalendarYearID <= {end_year}
  AND t.Status = 'Completed'
GROUP BY d.CalendarYearID, d.CalendarQtrID, d.CalendarMonthID
HAVING COUNT(t.TradeID) > {min_trades}
ORDER BY d.CalendarYearID, d.CalendarMonthID;
"""

        # AQ5: Portfolio analysis with risk and return calculations
        queries["AQ5"] = """
WITH price_returns AS (
    SELECT
        mh.SK_SecurityID,
        mh.SK_DateID,
        mh.ClosePrice,
        mh.PERatio,
        mh.Yield,
        (mh.ClosePrice / LAG(mh.ClosePrice) OVER (PARTITION BY mh.SK_SecurityID ORDER BY mh.SK_DateID) - 1) as daily_return
    FROM FactMarketHistory mh
)
SELECT
    'Portfolio Risk and Return Analysis' as analysis_name,
    c.SK_CustomerID,
    c.Tier,
    c.NetWorth,
    c.CreditRating,
    COUNT(DISTINCT h.SK_SecurityID) as portfolio_diversification,
    SUM(h.CurrentHolding * h.CurrentPrice) as total_portfolio_value,
    SUM(cb.Cash) as cash_balance,
    (SUM(h.CurrentHolding * h.CurrentPrice) + SUM(cb.Cash)) as total_account_value,
    SUM(h.CurrentHolding * h.CurrentPrice) / (SUM(h.CurrentHolding * h.CurrentPrice) + SUM(cb.Cash)) * 100 as equity_allocation_pct,
    SUM(cb.Cash) / (SUM(h.CurrentHolding * h.CurrentPrice) + SUM(cb.Cash)) * 100 as cash_allocation_pct,
    AVG(pr.PERatio) as avg_portfolio_pe_ratio,
    AVG(pr.Yield) as avg_portfolio_yield,
    STDDEV(pr.daily_return) * 100 as portfolio_volatility_pct,
    SUM(CASE WHEN comp.SPrating IN ('AAA', 'AA+', 'AA', 'AA-') THEN h.CurrentHolding * h.CurrentPrice ELSE 0 END) /
        SUM(h.CurrentHolding * h.CurrentPrice) * 100 as high_grade_allocation_pct,
    COUNT(fw.SK_SecurityID) as watchlist_securities
FROM DimCustomer c
LEFT JOIN FactHoldings h ON c.SK_CustomerID = h.SK_CustomerID
LEFT JOIN FactCashBalances cb ON c.SK_CustomerID = cb.SK_CustomerID
LEFT JOIN price_returns pr ON h.SK_SecurityID = pr.SK_SecurityID AND h.SK_DateID = pr.SK_DateID
LEFT JOIN DimSecurity s ON h.SK_SecurityID = s.SK_SecurityID
LEFT JOIN DimCompany comp ON s.SK_CompanyID = comp.SK_CompanyID
LEFT JOIN FactWatches fw ON c.SK_CustomerID = fw.SK_CustomerID
WHERE c.IsCurrent = 1
  AND s.IsCurrent = 1
  AND comp.IsCurrent = 1
  AND h.CurrentHolding > 0
GROUP BY c.SK_CustomerID, c.Tier, c.NetWorth, c.CreditRating
HAVING SUM(h.CurrentHolding * h.CurrentPrice) > {min_portfolio_value}
ORDER BY total_portfolio_value DESC
LIMIT {limit_rows};
"""

        # AQ6: Industry sector performance analysis
        queries["AQ6"] = """
SELECT
    'Industry Sector Performance Analysis' as analysis_name,
    comp.Industry,
    COUNT(DISTINCT comp.SK_CompanyID) as companies_in_sector,
    COUNT(DISTINCT s.SK_SecurityID) as securities_in_sector,
    COUNT(t.TradeID) as total_sector_trades,
    SUM(t.Quantity * t.TradePrice) as total_sector_value,
    AVG(t.TradePrice) as avg_sector_price,
    SUM(t.Quantity) as total_sector_volume,
    AVG(comp.MarketCap) as avg_market_cap,
    AVG(mh.PERatio) as avg_sector_pe_ratio,
    AVG(mh.Yield) as avg_sector_yield,
    AVG(mh.ClosePrice) as avg_closing_price,
    STDDEV(mh.ClosePrice) as price_volatility,
    COUNT(CASE WHEN comp.SPrating IN ('AAA', 'AA+', 'AA', 'AA-', 'A+', 'A', 'A-') THEN 1 END) as investment_grade_companies,
    COUNT(CASE WHEN comp.SPrating IN ('BBB+', 'BBB', 'BBB-', 'BB+', 'BB', 'BB-', 'B+', 'B', 'B-', 'CCC') THEN 1 END) as speculative_grade_companies,
    SUM(t.Quantity * t.TradePrice) / SUM(SUM(t.Quantity * t.TradePrice)) OVER () * 100 as sector_market_share_pct
FROM DimCompany comp
JOIN DimSecurity s ON comp.SK_CompanyID = s.SK_CompanyID
LEFT JOIN FactTrade t ON s.SK_SecurityID = t.SK_SecurityID
LEFT JOIN FactMarketHistory mh ON s.SK_SecurityID = mh.SK_SecurityID
LEFT JOIN DimDate d ON t.SK_CreateDateID = d.SK_DateID
WHERE comp.IsCurrent = 1
  AND s.IsCurrent = 1
  AND (d.CalendarYearID >= {start_year} AND d.CalendarYearID <= {end_year})
  AND t.Status = 'Completed'
GROUP BY comp.Industry
HAVING COUNT(t.TradeID) > {min_trades}
ORDER BY total_sector_value DESC, avg_sector_pe_ratio ASC
LIMIT {limit_rows};
"""

        # AQ7: Customer lifecycle and retention analysis
        queries["AQ7"] = """
SELECT
    'Customer Lifecycle and Retention Analysis' as analysis_name,
    customer_tenure_months,
    customer_count,
    avg_trades_per_customer,
    avg_trade_value_per_customer,
    avg_fees_per_customer,
    retention_rate_pct,
    avg_portfolio_value,
    avg_cash_balance
FROM (
    SELECT
        CASE
            WHEN months_since_first_trade <= 6 THEN '0-6 months'
            WHEN months_since_first_trade <= 12 THEN '7-12 months'
            WHEN months_since_first_trade <= 24 THEN '1-2 years'
            WHEN months_since_first_trade <= 36 THEN '2-3 years'
            ELSE '3+ years'
        END as customer_tenure_months,
        COUNT(DISTINCT c.SK_CustomerID) as customer_count,
        AVG(customer_metrics.total_trades) as avg_trades_per_customer,
        AVG(customer_metrics.total_trade_value) as avg_trade_value_per_customer,
        AVG(customer_metrics.total_fees) as avg_fees_per_customer,
        SUM(CASE WHEN customer_metrics.recent_activity = 1 THEN 1 ELSE 0 END) / COUNT(*) * 100 as retention_rate_pct,
        AVG(customer_metrics.portfolio_value) as avg_portfolio_value,
        AVG(customer_metrics.cash_balance) as avg_cash_balance
    FROM DimCustomer c
    JOIN (
        SELECT
            c.SK_CustomerID,
            MIN(d.DateValue) as first_trade_date,
            MAX(d.DateValue) as last_trade_date,
            JULIANDAY(DATE('now')) - JULIANDAY(MIN(d.DateValue)) as days_since_first_trade,
            (JULIANDAY(DATE('now')) - JULIANDAY(MIN(d.DateValue))) / 30.44 as months_since_first_trade,
            COUNT(t.TradeID) as total_trades,
            SUM(t.Quantity * t.TradePrice) as total_trade_value,
            SUM(t.Fee + t.Commission + t.Tax) as total_fees,
            CASE WHEN MAX(d.DateValue) >= DATE('now', '-90 days') THEN 1 ELSE 0 END as recent_activity,
            COALESCE(SUM(h.CurrentHolding * h.CurrentPrice), 0) as portfolio_value,
            COALESCE(SUM(cb.Cash), 0) as cash_balance
        FROM DimCustomer c
        LEFT JOIN FactTrade t ON c.SK_CustomerID = t.SK_CustomerID
        LEFT JOIN DimDate d ON t.SK_CreateDateID = d.SK_DateID
        LEFT JOIN FactHoldings h ON c.SK_CustomerID = h.SK_CustomerID
        LEFT JOIN FactCashBalances cb ON c.SK_CustomerID = cb.SK_CustomerID
        WHERE c.IsCurrent = 1 AND t.Status = 'Completed'
        GROUP BY c.SK_CustomerID
        HAVING COUNT(t.TradeID) > 0
    ) customer_metrics ON c.SK_CustomerID = customer_metrics.SK_CustomerID
    WHERE c.IsCurrent = 1
    GROUP BY
        CASE
            WHEN customer_metrics.months_since_first_trade <= 6 THEN '0-6 months'
            WHEN customer_metrics.months_since_first_trade <= 12 THEN '7-12 months'
            WHEN customer_metrics.months_since_first_trade <= 24 THEN '1-2 years'
            WHEN customer_metrics.months_since_first_trade <= 36 THEN '2-3 years'
            ELSE '3+ years'
        END
) tenure_analysis
ORDER BY
    CASE customer_tenure_months
        WHEN '0-6 months' THEN 1
        WHEN '7-12 months' THEN 2
        WHEN '1-2 years' THEN 3
        WHEN '2-3 years' THEN 4
        ELSE 5
    END;
"""

        # AQ8: Trading pattern analysis - High frequency vs long-term investors
        queries["AQ8"] = """
SELECT
    'Trading Pattern Analysis' as analysis_name,
    trading_frequency_profile,
    customer_count,
    avg_trades_per_customer,
    avg_trade_size,
    avg_holding_period_days,
    total_commission_generated,
    avg_portfolio_turnover_rate
FROM (
    SELECT
        CASE
            WHEN trades_per_month >= 50 THEN 'High Frequency (50+ trades/month)'
            WHEN trades_per_month >= 10 THEN 'Active (10-49 trades/month)'
            WHEN trades_per_month >= 2 THEN 'Regular (2-9 trades/month)'
            WHEN trades_per_month >= 0.5 THEN 'Occasional (0.5-2 trades/month)'
            ELSE 'Long-term (<0.5 trades/month)'
        END as trading_frequency_profile,
        COUNT(DISTINCT customer_id) as customer_count,
        AVG(total_trades) as avg_trades_per_customer,
        AVG(avg_trade_size) as avg_trade_size,
        AVG(avg_holding_period) as avg_holding_period_days,
        SUM(total_commission) as total_commission_generated,
        AVG(portfolio_turnover) as avg_portfolio_turnover_rate
    FROM (
        SELECT
            c.SK_CustomerID as customer_id,
            c.Tier,
            COUNT(t.TradeID) as total_trades,
            COUNT(t.TradeID) / GREATEST(JULIANDAY(MAX(d.DateValue)) - JULIANDAY(MIN(d.DateValue)), 1) * 30.44 as trades_per_month,
            AVG(t.Quantity * t.TradePrice) as avg_trade_size,
            AVG(CASE WHEN sell_date.DateValue IS NOT NULL
                     THEN JULIANDAY(sell_date.DateValue) - JULIANDAY(buy_date.DateValue)
                     ELSE NULL END) as avg_holding_period,
            SUM(t.Commission) as total_commission,
            COALESCE(SUM(CASE WHEN tt.TT_IS_SELL = 1 THEN t.Quantity * t.TradePrice ELSE 0 END) /
                     NULLIF(SUM(h.CurrentHolding * h.CurrentPrice), 0), 0) as portfolio_turnover
        FROM DimCustomer c
        LEFT JOIN FactTrade t ON c.SK_CustomerID = t.SK_CustomerID
        LEFT JOIN DimDate d ON t.SK_CreateDateID = d.SK_DateID
        LEFT JOIN TradeType tt ON t.Type = tt.TT_ID
        LEFT JOIN FactHoldings h ON c.SK_CustomerID = h.SK_CustomerID
        LEFT JOIN DimDate buy_date ON t.SK_CreateDateID = buy_date.SK_DateID AND tt.TT_IS_SELL = 0
        LEFT JOIN DimDate sell_date ON t.SK_CloseDateID = sell_date.SK_DateID AND tt.TT_IS_SELL = 1
        WHERE c.IsCurrent = 1
          AND t.Status = 'Completed'
          AND d.CalendarYearID >= {start_year}
        GROUP BY c.SK_CustomerID, c.Tier
        HAVING COUNT(t.TradeID) > {min_trades}
    ) customer_trading_patterns
    GROUP BY
        CASE
            WHEN trades_per_month >= 50 THEN 'High Frequency (50+ trades/month)'
            WHEN trades_per_month >= 10 THEN 'Active (10-49 trades/month)'
            WHEN trades_per_month >= 2 THEN 'Regular (2-9 trades/month)'
            WHEN trades_per_month >= 0.5 THEN 'Occasional (0.5-2 trades/month)'
            ELSE 'Long-term (<0.5 trades/month)'
        END
) pattern_analysis
ORDER BY
    CASE trading_frequency_profile
        WHEN 'High Frequency (50+ trades/month)' THEN 1
        WHEN 'Active (10-49 trades/month)' THEN 2
        WHEN 'Regular (2-9 trades/month)' THEN 3
        WHEN 'Occasional (0.5-2 trades/month)' THEN 4
        ELSE 5
    END;
"""

        # AQ9: Market maker analysis - Bid/ask spread and liquidity provision
        queries["AQ9"] = """
SELECT
    'Market Maker and Liquidity Analysis' as analysis_name,
    s.Symbol,
    s.Name as security_name,
    COUNT(DISTINCT t.SK_BrokerID) as market_makers,
    COUNT(CASE WHEN tt.TT_IS_SELL = 1 THEN 1 END) as total_sell_orders,
    COUNT(CASE WHEN tt.TT_IS_SELL = 0 THEN 1 END) as total_buy_orders,
    AVG(CASE WHEN tt.TT_IS_SELL = 1 THEN t.TradePrice END) as avg_sell_price,
    AVG(CASE WHEN tt.TT_IS_SELL = 0 THEN t.TradePrice END) as avg_buy_price,
    (AVG(CASE WHEN tt.TT_IS_SELL = 1 THEN t.TradePrice END) -
     AVG(CASE WHEN tt.TT_IS_SELL = 0 THEN t.TradePrice END)) as bid_ask_spread,
    (AVG(CASE WHEN tt.TT_IS_SELL = 1 THEN t.TradePrice END) -
     AVG(CASE WHEN tt.TT_IS_SELL = 0 THEN t.TradePrice END)) /
     AVG(t.TradePrice) * 100 as bid_ask_spread_pct,
    SUM(t.Quantity) as total_volume,
    AVG(mh.Volume) as avg_daily_volume,
    SUM(t.Quantity) / AVG(mh.Volume) as market_share_of_volume,
    STDDEV(t.TradePrice) as price_volatility,
    STDDEV(t.TradePrice) / AVG(t.TradePrice) * 100 as coefficient_of_variation
FROM DimSecurity s
JOIN FactTrade t ON s.SK_SecurityID = t.SK_SecurityID
JOIN TradeType tt ON t.Type = tt.TT_ID
JOIN FactMarketHistory mh ON s.SK_SecurityID = mh.SK_SecurityID
JOIN DimDate d ON t.SK_CreateDateID = d.SK_DateID
WHERE s.IsCurrent = 1
  AND t.Status = 'Completed'
  AND d.CalendarYearID >= {start_year}
  AND d.CalendarYearID <= {end_year}
GROUP BY s.Symbol, s.Name
HAVING COUNT(t.TradeID) > {min_trades}
  AND COUNT(CASE WHEN tt.TT_IS_SELL = 1 THEN 1 END) > 0
  AND COUNT(CASE WHEN tt.TT_IS_SELL = 0 THEN 1 END) > 0
ORDER BY market_share_of_volume DESC, bid_ask_spread_pct ASC
LIMIT {limit_rows};
"""

        # AQ10: Regulatory compliance and risk monitoring
        queries["AQ10"] = """
SELECT
    'Regulatory Compliance and Risk Monitoring' as analysis_name,
    c.SK_CustomerID,
    c.Tier,
    c.Country,
    COUNT(t.TradeID) as total_trades,
    SUM(t.Quantity * t.TradePrice) as total_trade_value,
    MAX(t.Quantity * t.TradePrice) as largest_single_trade,
    COUNT(CASE WHEN t.Quantity * t.TradePrice > {large_trade_threshold} THEN 1 END) as large_trades,
    COUNT(CASE WHEN d.DateValue = current_date THEN 1 END) as same_day_trades,
    COUNT(DISTINCT t.SK_SecurityID) as securities_traded,
    SUM(h.CurrentHolding * h.CurrentPrice) as current_portfolio_value,
    SUM(h.CurrentHolding * h.CurrentPrice) / c.NetWorth * 100 as portfolio_to_networth_pct,
    COUNT(CASE WHEN wash_sales.wash_sale_flag = 1 THEN 1 END) as potential_wash_sales,
    AVG(t.Commission) / AVG(t.Quantity * t.TradePrice) * 100 as avg_commission_rate_pct,
    CASE
        WHEN COUNT(CASE WHEN t.Quantity * t.TradePrice > {large_trade_threshold} THEN 1 END) > {large_trade_count_threshold} THEN 'HIGH RISK'
        WHEN SUM(h.CurrentHolding * h.CurrentPrice) / c.NetWorth > 0.8 THEN 'CONCENTRATION RISK'
        WHEN COUNT(CASE WHEN wash_sales.wash_sale_flag = 1 THEN 1 END) > 0 THEN 'WASH SALE RISK'
        WHEN COUNT(CASE WHEN d.DateValue = current_date THEN 1 END) > {same_day_trade_threshold} THEN 'DAY TRADING RISK'
        ELSE 'NORMAL'
    END as risk_profile
FROM DimCustomer c
LEFT JOIN FactTrade t ON c.SK_CustomerID = t.SK_CustomerID
LEFT JOIN FactHoldings h ON c.SK_CustomerID = h.SK_CustomerID
LEFT JOIN DimDate d ON t.SK_CreateDateID = d.SK_DateID
LEFT JOIN (
    SELECT
        t1.SK_CustomerID,
        t1.SK_SecurityID,
        t1.TradeID,
        CASE WHEN t2.TradeID IS NOT NULL THEN 1 ELSE 0 END as wash_sale_flag
    FROM FactTrade t1
    LEFT JOIN DimDate t1_date ON t1.SK_CreateDateID = t1_date.SK_DateID
    LEFT JOIN FactTrade t2 ON t1.SK_CustomerID = t2.SK_CustomerID
                          AND t1.SK_SecurityID = t2.SK_SecurityID
                          AND t1.TradeID != t2.TradeID
    LEFT JOIN DimDate t2_date ON t2.SK_CreateDateID = t2_date.SK_DateID
                              AND ABS(JULIANDAY(t1_date.DateValue) - JULIANDAY(t2_date.DateValue)) <= 30
) wash_sales ON t.TradeID = wash_sales.TradeID
WHERE c.IsCurrent = 1
  AND t.Status = 'Completed'
  AND d.CalendarYearID >= {start_year}
GROUP BY c.SK_CustomerID, c.Tier, c.Country, c.NetWorth
HAVING COUNT(t.TradeID) > {min_trades}
ORDER BY
    CASE risk_profile
        WHEN 'HIGH RISK' THEN 1
        WHEN 'CONCENTRATION RISK' THEN 2
        WHEN 'WASH SALE RISK' THEN 3
        WHEN 'DAY TRADING RISK' THEN 4
        ELSE 5
    END,
    total_trade_value DESC
LIMIT {limit_rows};
"""

        return queries

    def _load_analytical_metadata(self) -> dict[str, dict[str, Any]]:
        """Load metadata for analytical queries.

        Returns:
            Dictionary mapping query IDs to their metadata
        """
        metadata = {}

        metadata["AQ1"] = {
            "relies_on": ["DimCustomer", "FactTrade", "DimDate"],
            "query_type": "analytical",
            "category": "customer_profitability",
            "description": "Customer profitability analysis by tier and demographics",
            "complexity": "medium",
        }

        metadata["AQ2"] = {
            "relies_on": [
                "DimSecurity",
                "DimCompany",
                "FactTrade",
                "FactMarketHistory",
                "DimDate",
            ],
            "query_type": "analytical",
            "category": "security_performance",
            "description": "Security performance analysis with price movements and volatility",
            "complexity": "high",
        }

        metadata["AQ3"] = {
            "relies_on": ["DimBroker", "FactTrade", "TradeType", "DimDate"],
            "query_type": "analytical",
            "category": "broker_performance",
            "description": "Broker performance analysis with commission and trade volume metrics",
            "complexity": "medium",
        }

        metadata["AQ4"] = {
            "relies_on": ["DimDate", "FactTrade", "FactMarketHistory", "TradeType"],
            "query_type": "analytical",
            "category": "market_trends",
            "description": "Market trend analysis with time-series aggregations",
            "complexity": "high",
        }

        metadata["AQ5"] = {
            "relies_on": [
                "DimCustomer",
                "FactHoldings",
                "FactCashBalances",
                "FactMarketHistory",
                "DimSecurity",
                "DimCompany",
                "FactWatches",
            ],
            "query_type": "analytical",
            "category": "portfolio_analysis",
            "description": "Portfolio analysis with risk and return calculations",
            "complexity": "high",
        }

        metadata["AQ6"] = {
            "relies_on": [
                "DimCompany",
                "DimSecurity",
                "FactTrade",
                "FactMarketHistory",
                "DimDate",
            ],
            "query_type": "analytical",
            "category": "industry_analysis",
            "description": "Industry sector performance analysis",
            "complexity": "medium",
        }

        metadata["AQ7"] = {
            "relies_on": [
                "DimCustomer",
                "FactTrade",
                "DimDate",
                "FactHoldings",
                "FactCashBalances",
            ],
            "query_type": "analytical",
            "category": "customer_lifecycle",
            "description": "Customer lifecycle and retention analysis",
            "complexity": "high",
        }

        metadata["AQ8"] = {
            "relies_on": [
                "DimCustomer",
                "FactTrade",
                "DimDate",
                "TradeType",
                "FactHoldings",
            ],
            "query_type": "analytical",
            "category": "trading_patterns",
            "description": "Trading pattern analysis - High frequency vs long-term investors",
            "complexity": "high",
        }

        metadata["AQ9"] = {
            "relies_on": [
                "DimSecurity",
                "FactTrade",
                "TradeType",
                "FactMarketHistory",
                "DimDate",
            ],
            "query_type": "analytical",
            "category": "market_microstructure",
            "description": "Market maker analysis - Bid/ask spread and liquidity provision",
            "complexity": "high",
        }

        metadata["AQ10"] = {
            "relies_on": ["DimCustomer", "FactTrade", "FactHoldings", "DimDate"],
            "query_type": "analytical",
            "category": "risk_compliance",
            "description": "Regulatory compliance and risk monitoring",
            "complexity": "high",
        }

        return metadata

    def get_query(self, query_id: str, params: Optional[dict[str, Any]] = None) -> str:
        """Get an analytical query with parameters.

        Args:
            query_id: Query identifier (AQ1, AQ2, etc.)
            params: Optional parameter values. If None, uses defaults.

        Returns:
            SQL query with parameters replaced

        Raises:
            ValueError: If query_id is invalid
        """
        if query_id not in self._queries:
            available = ", ".join(sorted(self._queries.keys()))
            raise ValueError(f"Invalid analytical query ID: {query_id}. Available: {available}")

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
        """Get all analytical queries with default parameters.

        Returns:
            Dictionary mapping query IDs to parameterized SQL
        """
        result = {}
        for query_id in self._queries:
            result[query_id] = self.get_query(query_id)
        return result

    def _generate_default_params(self, query_id: str) -> dict[str, Any]:
        """Generate default parameters for analytical queries.

        Args:
            query_id: Query identifier

        Returns:
            Dictionary of parameter names to default values
        """
        # Default parameters for analytical queries
        defaults = {
            # Time ranges (TPC-DI typically covers 5 years)
            "start_year": 2015,
            "end_year": 2019,
            # Minimum thresholds
            "min_trades": 10,
            "min_portfolio_value": 10000.00,
            # Risk and compliance thresholds
            "large_trade_threshold": 100000.00,  # $100K trades
            "large_trade_count_threshold": 10,  # More than 10 large trades = high risk
            "same_day_trade_threshold": 4,  # Day trading pattern detection
            # Result limits
            "limit_rows": 50,
        }

        return defaults

    def get_query_metadata(self, query_id: str) -> dict[str, Any]:
        """Get metadata for an analytical query.

        Args:
            query_id: Query identifier (AQ1, AQ2, etc.)

        Returns:
            Dictionary containing query metadata

        Raises:
            ValueError: If query_id is invalid
        """
        if query_id not in self._query_metadata:
            available = ", ".join(sorted(self._query_metadata.keys()))
            raise ValueError(f"Invalid analytical query ID: {query_id}. Available: {available}")

        return self._query_metadata[query_id].copy()

    def get_queries_by_category(self, category: str) -> list[str]:
        """Get all analytical queries of a specific category.

        Args:
            category: Category name (customer_profitability, security_performance,
                     broker_performance, market_trends, portfolio_analysis, etc.)

        Returns:
            List of query IDs in the specified category
        """
        valid_categories = {
            "customer_profitability",
            "security_performance",
            "broker_performance",
            "market_trends",
            "portfolio_analysis",
            "industry_analysis",
            "customer_lifecycle",
            "trading_patterns",
            "market_microstructure",
            "risk_compliance",
        }
        if category not in valid_categories:
            raise ValueError(f"Invalid category: {category}. Valid categories: {', '.join(valid_categories)}")

        return [query_id for query_id, metadata in self._query_metadata.items() if metadata["category"] == category]

    def get_queries_by_complexity(self, complexity: str) -> list[str]:
        """Get all analytical queries of a specific complexity level.

        Args:
            complexity: Complexity level (low, medium, high)

        Returns:
            List of query IDs with the specified complexity
        """
        valid_complexities = {"low", "medium", "high"}
        if complexity not in valid_complexities:
            raise ValueError(f"Invalid complexity: {complexity}. Valid complexities: {', '.join(valid_complexities)}")

        return [query_id for query_id, metadata in self._query_metadata.items() if metadata["complexity"] == complexity]
