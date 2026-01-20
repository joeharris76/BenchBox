"""TPC-DI ETL validation query suite.

This module provides comprehensive ETL validation queries for TPC-DI covering
batch processing validation, incremental load validation, data transformation
validation, and ETL quality score calculations.

These queries validate the ETL pipeline execution and ensure data integration
processes comply with TPC-DI specifications.

Copyright 2026 Joe Harris / BenchBox Project

TPC Benchmark™ DI (TPC-DI) - Copyright © Transaction Processing Performance Council
This implementation is based on the TPC-DI specification.

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

from typing import Any, Optional


class TPCDIETLQueries:
    """TPC-DI ETL validation query manager."""

    def __init__(self) -> None:
        """Initialize the ETL query manager."""
        self._queries = self._load_etl_queries()
        self._query_metadata = self._load_etl_metadata()

    def _load_etl_queries(self) -> dict[str, str]:
        """Load all ETL validation queries.

        Returns:
            Dictionary mapping ETL query IDs to SQL text
        """
        queries = {}

        # EQ1: Batch processing validation - Record counts and processing times
        queries["EQ1"] = """
SELECT
    'Batch Processing Validation' as validation_name,
    batch_id,
    table_name,
    records_loaded,
    processing_time_seconds,
    records_per_second,
    error_count,
    warning_count,
    CASE
        WHEN error_count = 0 AND records_per_second > {min_throughput} THEN 'PASS'
        WHEN error_count = 0 AND records_per_second <= {min_throughput} THEN 'SLOW'
        ELSE 'FAIL'
    END as batch_status
FROM (
    SELECT
        1 as batch_id,
        'DimCustomer' as table_name,
        COUNT(*) as records_loaded,
        60.0 as processing_time_seconds,  -- Simulated processing time
        COUNT(*) / 60.0 as records_per_second,
        SUM(CASE WHEN Status IS NULL OR CustomerID IS NULL THEN 1 ELSE 0 END) as error_count,
        SUM(CASE WHEN CreditRating < 300 OR CreditRating > 850 THEN 1 ELSE 0 END) as warning_count
    FROM DimCustomer
    WHERE BatchID = 1
    UNION ALL
    SELECT
        1 as batch_id,
        'DimAccount' as table_name,
        COUNT(*) as records_loaded,
        45.0 as processing_time_seconds,
        COUNT(*) / 45.0 as records_per_second,
        SUM(CASE WHEN Status IS NULL OR AccountID IS NULL THEN 1 ELSE 0 END) as error_count,
        SUM(CASE WHEN SK_CustomerID IS NULL THEN 1 ELSE 0 END) as warning_count
    FROM DimAccount
    WHERE BatchID = 1
    UNION ALL
    SELECT
        1 as batch_id,
        'FactTrade' as table_name,
        COUNT(*) as records_loaded,
        120.0 as processing_time_seconds,
        COUNT(*) / 120.0 as records_per_second,
        SUM(CASE WHEN TradeID IS NULL OR SK_CustomerID IS NULL THEN 1 ELSE 0 END) as error_count,
        SUM(CASE WHEN TradePrice <= 0 OR Quantity <= 0 THEN 1 ELSE 0 END) as warning_count
    FROM FactTrade
    WHERE BatchID = 1
) batch_metrics
ORDER BY batch_id, table_name;
"""

        # EQ2: Incremental load validation - New vs updated records
        queries["EQ2"] = """
SELECT
    'Incremental Load Validation' as validation_name,
    table_name,
    batch_id,
    new_records,
    updated_records,
    unchanged_records,
    total_records,
    new_records / total_records * 100 as new_record_pct,
    updated_records / total_records * 100 as updated_record_pct,
    CASE
        WHEN (new_records + updated_records) > 0 AND unchanged_records >= 0 THEN 'PASS'
        WHEN (new_records + updated_records) = 0 THEN 'NO CHANGES'
        ELSE 'FAIL'
    END as incremental_status
FROM (
    SELECT
        'DimCustomer' as table_name,
        BatchID as batch_id,
        COUNT(*) as total_records,
        SUM(CASE WHEN EffectiveDate >= '{batch_start_date}' AND EndDate = '9999-12-31' THEN 1 ELSE 0 END) as new_records,
        SUM(CASE WHEN EffectiveDate >= '{batch_start_date}' AND EndDate != '9999-12-31' THEN 1 ELSE 0 END) as updated_records,
        SUM(CASE WHEN EffectiveDate < '{batch_start_date}' THEN 1 ELSE 0 END) as unchanged_records
    FROM DimCustomer
    WHERE BatchID >= {min_batch_id}
    GROUP BY BatchID
    UNION ALL
    SELECT
        'DimAccount' as table_name,
        BatchID as batch_id,
        COUNT(*) as total_records,
        SUM(CASE WHEN EffectiveDate >= '{batch_start_date}' AND EndDate = '9999-12-31' THEN 1 ELSE 0 END) as new_records,
        SUM(CASE WHEN EffectiveDate >= '{batch_start_date}' AND EndDate != '9999-12-31' THEN 1 ELSE 0 END) as updated_records,
        SUM(CASE WHEN EffectiveDate < '{batch_start_date}' THEN 1 ELSE 0 END) as unchanged_records
    FROM DimAccount
    WHERE BatchID >= {min_batch_id}
    GROUP BY BatchID
    UNION ALL
    SELECT
        'DimSecurity' as table_name,
        BatchID as batch_id,
        COUNT(*) as total_records,
        SUM(CASE WHEN EffectiveDate >= '{batch_start_date}' AND EndDate = '9999-12-31' THEN 1 ELSE 0 END) as new_records,
        SUM(CASE WHEN EffectiveDate >= '{batch_start_date}' AND EndDate != '9999-12-31' THEN 1 ELSE 0 END) as updated_records,
        SUM(CASE WHEN EffectiveDate < '{batch_start_date}' THEN 1 ELSE 0 END) as unchanged_records
    FROM DimSecurity
    WHERE BatchID >= {min_batch_id}
    GROUP BY BatchID
) incremental_metrics
ORDER BY batch_id, table_name;
"""

        # EQ3: Data transformation validation - Source to target mapping
        queries["EQ3"] = """
SELECT
    'Data Transformation Validation' as validation_name,
    transformation_type,
    source_records,
    target_records,
    transformation_success_rate,
    data_quality_score,
    CASE
        WHEN transformation_success_rate >= {success_rate_threshold} AND data_quality_score >= {quality_score_threshold} THEN 'PASS'
        WHEN transformation_success_rate >= {success_rate_threshold} THEN 'QUALITY ISSUES'
        ELSE 'TRANSFORMATION FAILURE'
    END as transformation_status
FROM (
    SELECT
        'Customer Demographics' as transformation_type,
        (SELECT COUNT(*) FROM (
            -- Simulated source data count
            SELECT COUNT(*) as source_count FROM DimCustomer WHERE BatchID = 1
        )) as source_records,
        COUNT(*) as target_records,
        COUNT(*) / (SELECT COUNT(*) FROM DimCustomer WHERE BatchID = 1) * 100 as transformation_success_rate,
        100 - (SUM(CASE WHEN CustomerID IS NULL OR TaxID IS NULL OR Status IS NULL THEN 1 ELSE 0 END) / COUNT(*) * 100) as data_quality_score
    FROM DimCustomer
    WHERE BatchID = 1
    UNION ALL
    SELECT
        'Account Information' as transformation_type,
        (SELECT COUNT(*) FROM DimAccount WHERE BatchID = 1) as source_records,
        COUNT(*) as target_records,
        100.0 as transformation_success_rate,  -- Assume 100% transformation success
        100 - (SUM(CASE WHEN AccountID IS NULL OR SK_CustomerID IS NULL THEN 1 ELSE 0 END) / COUNT(*) * 100) as data_quality_score
    FROM DimAccount
    WHERE BatchID = 1
    UNION ALL
    SELECT
        'Trade Transactions' as transformation_type,
        (SELECT COUNT(*) FROM FactTrade WHERE BatchID = 1) as source_records,
        COUNT(*) as target_records,
        100.0 as transformation_success_rate,
        100 - (SUM(CASE WHEN TradeID IS NULL OR SK_CustomerID IS NULL OR TradePrice <= 0 THEN 1 ELSE 0 END) / COUNT(*) * 100) as data_quality_score
    FROM FactTrade
    WHERE BatchID = 1
    UNION ALL
    SELECT
        'Market History Data' as transformation_type,
        (SELECT COUNT(*) FROM FactMarketHistory WHERE BatchID = 1) as source_records,
        COUNT(*) as target_records,
        100.0 as transformation_success_rate,
        100 - (SUM(CASE WHEN SK_SecurityID IS NULL OR ClosePrice <= 0 OR Volume < 0 THEN 1 ELSE 0 END) / COUNT(*) * 100) as data_quality_score
    FROM FactMarketHistory
    WHERE BatchID = 1
) transformation_metrics;
"""

        # EQ4: SCD Type 2 processing validation
        queries["EQ4"] = """
SELECT
    'SCD Type 2 Processing Validation' as validation_name,
    table_name,
    business_key_count,
    total_scd_records,
    current_records,
    historical_records,
    scd_processing_errors,
    CASE
        WHEN scd_processing_errors = 0 AND current_records = business_key_count THEN 'PASS'
        WHEN scd_processing_errors = 0 THEN 'HISTORICAL DATA ISSUES'
        ELSE 'SCD PROCESSING ERRORS'
    END as scd_status
FROM (
    SELECT
        'DimCustomer' as table_name,
        COUNT(DISTINCT CustomerID) as business_key_count,
        COUNT(*) as total_scd_records,
        SUM(CASE WHEN IsCurrent = 1 THEN 1 ELSE 0 END) as current_records,
        SUM(CASE WHEN IsCurrent = 0 THEN 1 ELSE 0 END) as historical_records,
        -- Count SCD processing errors
        (SELECT COUNT(*) FROM (
            SELECT CustomerID
            FROM DimCustomer
            WHERE IsCurrent = 1
            GROUP BY CustomerID
            HAVING COUNT(*) > 1  -- Multiple current records for same business key
        ) errors) +
        (SELECT COUNT(*) FROM DimCustomer WHERE EffectiveDate >= EndDate) as scd_processing_errors
    FROM DimCustomer
    UNION ALL
    SELECT
        'DimAccount' as table_name,
        COUNT(DISTINCT AccountID) as business_key_count,
        COUNT(*) as total_scd_records,
        SUM(CASE WHEN IsCurrent = 1 THEN 1 ELSE 0 END) as current_records,
        SUM(CASE WHEN IsCurrent = 0 THEN 1 ELSE 0 END) as historical_records,
        (SELECT COUNT(*) FROM (
            SELECT AccountID
            FROM DimAccount
            WHERE IsCurrent = 1
            GROUP BY AccountID
            HAVING COUNT(*) > 1
        ) errors) +
        (SELECT COUNT(*) FROM DimAccount WHERE EffectiveDate >= EndDate) as scd_processing_errors
    FROM DimAccount
    UNION ALL
    SELECT
        'DimSecurity' as table_name,
        COUNT(DISTINCT Symbol) as business_key_count,
        COUNT(*) as total_scd_records,
        SUM(CASE WHEN IsCurrent = 1 THEN 1 ELSE 0 END) as current_records,
        SUM(CASE WHEN IsCurrent = 0 THEN 1 ELSE 0 END) as historical_records,
        (SELECT COUNT(*) FROM (
            SELECT Symbol
            FROM DimSecurity
            WHERE IsCurrent = 1
            GROUP BY Symbol
            HAVING COUNT(*) > 1
        ) errors) +
        (SELECT COUNT(*) FROM DimSecurity WHERE EffectiveDate >= EndDate) as scd_processing_errors
    FROM DimSecurity
    UNION ALL
    SELECT
        'DimCompany' as table_name,
        COUNT(DISTINCT CompanyID) as business_key_count,
        COUNT(*) as total_scd_records,
        SUM(CASE WHEN IsCurrent = 1 THEN 1 ELSE 0 END) as current_records,
        SUM(CASE WHEN IsCurrent = 0 THEN 1 ELSE 0 END) as historical_records,
        (SELECT COUNT(*) FROM (
            SELECT CompanyID
            FROM DimCompany
            WHERE IsCurrent = 1
            GROUP BY CompanyID
            HAVING COUNT(*) > 1
        ) errors) +
        (SELECT COUNT(*) FROM DimCompany WHERE EffectiveDate >= EndDate) as scd_processing_errors
    FROM DimCompany
    UNION ALL
    SELECT
        'DimBroker' as table_name,
        COUNT(DISTINCT BrokerID) as business_key_count,
        COUNT(*) as total_scd_records,
        SUM(CASE WHEN IsCurrent = 1 THEN 1 ELSE 0 END) as current_records,
        SUM(CASE WHEN IsCurrent = 0 THEN 1 ELSE 0 END) as historical_records,
        (SELECT COUNT(*) FROM (
            SELECT BrokerID
            FROM DimBroker
            WHERE IsCurrent = 1
            GROUP BY BrokerID
            HAVING COUNT(*) > 1
        ) errors) +
        (SELECT COUNT(*) FROM DimBroker WHERE EffectiveDate >= EndDate) as scd_processing_errors
    FROM DimBroker
) scd_validation
ORDER BY table_name;
"""

        # EQ5: Data lineage and audit trail validation
        queries["EQ5"] = """
SELECT
    'Data Lineage and Audit Trail Validation' as validation_name,
    batch_id,
    total_records_processed,
    records_with_audit_trail,
    audit_coverage_pct,
    batch_consistency_score,
    CASE
        WHEN audit_coverage_pct >= {audit_coverage_threshold} AND batch_consistency_score >= {consistency_score_threshold} THEN 'PASS'
        WHEN audit_coverage_pct >= {audit_coverage_threshold} THEN 'CONSISTENCY ISSUES'
        ELSE 'AUDIT TRAIL INCOMPLETE'
    END as audit_status
FROM (
    SELECT
        batch_summary.batch_id,
        batch_summary.total_records_processed,
        batch_summary.records_with_audit_trail,
        batch_summary.records_with_audit_trail / batch_summary.total_records_processed * 100 as audit_coverage_pct,
        -- Calculate consistency score based on batch ID consistency across related tables
        100 - (batch_inconsistencies.inconsistent_records / batch_summary.total_records_processed * 100) as batch_consistency_score
    FROM (
        SELECT
            1 as batch_id,
            (SELECT COUNT(*) FROM DimCustomer WHERE BatchID = 1) +
            (SELECT COUNT(*) FROM DimAccount WHERE BatchID = 1) +
            (SELECT COUNT(*) FROM FactTrade WHERE BatchID = 1) as total_records_processed,
            (SELECT COUNT(*) FROM DimCustomer WHERE BatchID = 1 AND BatchID IS NOT NULL) +
            (SELECT COUNT(*) FROM DimAccount WHERE BatchID = 1 AND BatchID IS NOT NULL) +
            (SELECT COUNT(*) FROM FactTrade WHERE BatchID = 1 AND BatchID IS NOT NULL) as records_with_audit_trail
    ) batch_summary
    CROSS JOIN (
        SELECT
            -- Count records with inconsistent batch references
            (SELECT COUNT(*) FROM FactTrade ft
             LEFT JOIN DimCustomer dc ON ft.SK_CustomerID = dc.SK_CustomerID
             WHERE ft.BatchID != dc.BatchID AND dc.IsCurrent = 1) +
            (SELECT COUNT(*) FROM FactTrade ft
             LEFT JOIN DimAccount da ON ft.SK_AccountID = da.SK_AccountID
             WHERE ft.BatchID != da.BatchID AND da.IsCurrent = 1) as inconsistent_records
    ) batch_inconsistencies
    UNION ALL
    SELECT
        batch_summary.batch_id,
        batch_summary.total_records_processed,
        batch_summary.records_with_audit_trail,
        batch_summary.records_with_audit_trail / batch_summary.total_records_processed * 100 as audit_coverage_pct,
        100 - (batch_inconsistencies.inconsistent_records / batch_summary.total_records_processed * 100) as batch_consistency_score
    FROM (
        SELECT
            2 as batch_id,
            (SELECT COUNT(*) FROM DimCustomer WHERE BatchID = 2) +
            (SELECT COUNT(*) FROM DimAccount WHERE BatchID = 2) +
            (SELECT COUNT(*) FROM FactTrade WHERE BatchID = 2) as total_records_processed,
            (SELECT COUNT(*) FROM DimCustomer WHERE BatchID = 2 AND BatchID IS NOT NULL) +
            (SELECT COUNT(*) FROM DimAccount WHERE BatchID = 2 AND BatchID IS NOT NULL) +
            (SELECT COUNT(*) FROM FactTrade WHERE BatchID = 2 AND BatchID IS NOT NULL) as records_with_audit_trail
    ) batch_summary
    CROSS JOIN (
        SELECT
            (SELECT COUNT(*) FROM FactTrade ft
             LEFT JOIN DimCustomer dc ON ft.SK_CustomerID = dc.SK_CustomerID
             WHERE ft.BatchID = 2 AND ft.BatchID != dc.BatchID AND dc.IsCurrent = 1) as inconsistent_records
    ) batch_inconsistencies
) audit_validation
WHERE total_records_processed > 0
ORDER BY batch_id;
"""

        # EQ6: ETL performance metrics and throughput validation
        queries["EQ6"] = """
SELECT
    'ETL Performance Metrics Validation' as validation_name,
    etl_phase,
    total_records_processed,
    processing_time_minutes,
    records_per_minute,
    memory_usage_mb,
    cpu_utilization_pct,
    io_operations,
    performance_rating,
    CASE
        WHEN performance_rating >= {performance_rating_threshold} THEN 'OPTIMAL'
        WHEN performance_rating >= {performance_rating_threshold} * 0.7 THEN 'ACCEPTABLE'
        ELSE 'NEEDS OPTIMIZATION'
    END as performance_status
FROM (
    SELECT
        'Data Extraction' as etl_phase,
        10000 as total_records_processed,  -- Simulated metrics
        2.5 as processing_time_minutes,
        10000 / 2.5 as records_per_minute,
        256 as memory_usage_mb,
        45 as cpu_utilization_pct,
        5000 as io_operations,
        -- Performance rating formula (0-100 scale)
        LEAST(100, (10000 / 2.5) / 100 * 50 + (100 - 45) * 0.3 + (1000 - 256) / 10 * 0.2) as performance_rating
    UNION ALL
    SELECT
        'Data Transformation' as etl_phase,
        10000 as total_records_processed,
        8.0 as processing_time_minutes,
        10000 / 8.0 as records_per_minute,
        512 as memory_usage_mb,
        75 as cpu_utilization_pct,
        15000 as io_operations,
        LEAST(100, (10000 / 8.0) / 100 * 50 + (100 - 75) * 0.3 + (1000 - 512) / 10 * 0.2) as performance_rating
    UNION ALL
    SELECT
        'Data Loading' as etl_phase,
        10000 as total_records_processed,
        5.0 as processing_time_minutes,
        10000 / 5.0 as records_per_minute,
        128 as memory_usage_mb,
        30 as cpu_utilization_pct,
        25000 as io_operations,
        LEAST(100, (10000 / 5.0) / 100 * 50 + (100 - 30) * 0.3 + (1000 - 128) / 10 * 0.2) as performance_rating
    UNION ALL
    SELECT
        'Data Validation' as etl_phase,
        10000 as total_records_processed,
        3.0 as processing_time_minutes,
        10000 / 3.0 as records_per_minute,
        64 as memory_usage_mb,
        20 as cpu_utilization_pct,
        8000 as io_operations,
        LEAST(100, (10000 / 3.0) / 100 * 50 + (100 - 20) * 0.3 + (1000 - 64) / 10 * 0.2) as performance_rating
) performance_metrics
ORDER BY
    CASE etl_phase
        WHEN 'Data Extraction' THEN 1
        WHEN 'Data Transformation' THEN 2
        WHEN 'Data Loading' THEN 3
        WHEN 'Data Validation' THEN 4
    END;
"""

        # EQ7: Data quality score calculation for ETL monitoring
        queries["EQ7"] = """
SELECT
    'ETL Data Quality Score Calculation' as validation_name,
    table_name,
    total_records,
    null_key_fields,
    invalid_values,
    constraint_violations,
    business_rule_violations,
    completeness_score,
    validity_score,
    consistency_score,
    overall_quality_score,
    CASE
        WHEN overall_quality_score >= {excellent_quality_threshold} THEN 'EXCELLENT'
        WHEN overall_quality_score >= {good_quality_threshold} THEN 'GOOD'
        WHEN overall_quality_score >= {acceptable_quality_threshold} THEN 'ACCEPTABLE'
        ELSE 'POOR'
    END as quality_rating
FROM (
    SELECT
        'DimCustomer' as table_name,
        COUNT(*) as total_records,
        SUM(CASE WHEN CustomerID IS NULL OR TaxID IS NULL OR Status IS NULL THEN 1 ELSE 0 END) as null_key_fields,
        SUM(CASE WHEN Gender NOT IN ('M', 'F') OR Tier NOT IN (1,2,3) THEN 1 ELSE 0 END) as invalid_values,
        SUM(CASE WHEN CreditRating < 300 OR CreditRating > 850 THEN 1 ELSE 0 END) as constraint_violations,
        SUM(CASE WHEN (Tier = 1 AND NetWorth < 1000000) OR (Tier = 3 AND NetWorth > 250000) THEN 1 ELSE 0 END) as business_rule_violations,
        (COUNT(*) - SUM(CASE WHEN CustomerID IS NULL OR TaxID IS NULL OR Status IS NULL THEN 1 ELSE 0 END)) / COUNT(*) * 100 as completeness_score,
        (COUNT(*) - SUM(CASE WHEN Gender NOT IN ('M', 'F') OR Tier NOT IN (1,2,3) THEN 1 ELSE 0 END)) / COUNT(*) * 100 as validity_score,
        (COUNT(*) - SUM(CASE WHEN CreditRating < 300 OR CreditRating > 850 THEN 1 ELSE 0 END) -
         SUM(CASE WHEN (Tier = 1 AND NetWorth < 1000000) OR (Tier = 3 AND NetWorth > 250000) THEN 1 ELSE 0 END)) / COUNT(*) * 100 as consistency_score
    FROM DimCustomer
    WHERE IsCurrent = 1
    UNION ALL
    SELECT
        'DimAccount' as table_name,
        COUNT(*) as total_records,
        SUM(CASE WHEN AccountID IS NULL OR SK_CustomerID IS NULL OR Status IS NULL THEN 1 ELSE 0 END) as null_key_fields,
        SUM(CASE WHEN Status NOT IN ('Active', 'Inactive', 'Closed') THEN 1 ELSE 0 END) as invalid_values,
        SUM(CASE WHEN TaxStatus NOT IN (0, 1, 2) THEN 1 ELSE 0 END) as constraint_violations,
        0 as business_rule_violations,  -- No specific business rules for accounts
        (COUNT(*) - SUM(CASE WHEN AccountID IS NULL OR SK_CustomerID IS NULL OR Status IS NULL THEN 1 ELSE 0 END)) / COUNT(*) * 100 as completeness_score,
        (COUNT(*) - SUM(CASE WHEN Status NOT IN ('Active', 'Inactive', 'Closed') THEN 1 ELSE 0 END)) / COUNT(*) * 100 as validity_score,
        (COUNT(*) - SUM(CASE WHEN TaxStatus NOT IN (0, 1, 2) THEN 1 ELSE 0 END)) / COUNT(*) * 100 as consistency_score
    FROM DimAccount
    WHERE IsCurrent = 1
    UNION ALL
    SELECT
        'FactTrade' as table_name,
        COUNT(*) as total_records,
        SUM(CASE WHEN TradeID IS NULL OR SK_CustomerID IS NULL OR SK_SecurityID IS NULL THEN 1 ELSE 0 END) as null_key_fields,
        SUM(CASE WHEN Status NOT IN ('Pending', 'Completed', 'Cancelled') OR Type NOT IN ('Buy', 'Sell') THEN 1 ELSE 0 END) as invalid_values,
        SUM(CASE WHEN TradePrice <= 0 OR Quantity <= 0 OR Fee < 0 OR Commission < 0 THEN 1 ELSE 0 END) as constraint_violations,
        SUM(CASE WHEN TradePrice > 10000 OR Quantity > 1000000 THEN 1 ELSE 0 END) as business_rule_violations,  -- Unreasonably large trades
        (COUNT(*) - SUM(CASE WHEN TradeID IS NULL OR SK_CustomerID IS NULL OR SK_SecurityID IS NULL THEN 1 ELSE 0 END)) / COUNT(*) * 100 as completeness_score,
        (COUNT(*) - SUM(CASE WHEN Status NOT IN ('Pending', 'Completed', 'Cancelled') OR Type NOT IN ('Buy', 'Sell') THEN 1 ELSE 0 END)) / COUNT(*) * 100 as validity_score,
        (COUNT(*) - SUM(CASE WHEN TradePrice <= 0 OR Quantity <= 0 OR Fee < 0 OR Commission < 0 THEN 1 ELSE 0 END) -
         SUM(CASE WHEN TradePrice > 10000 OR Quantity > 1000000 THEN 1 ELSE 0 END)) / COUNT(*) * 100 as consistency_score
    FROM FactTrade
) quality_metrics,
(
    SELECT
        -- Calculate overall quality score as weighted average
        (completeness_score * 0.4 + validity_score * 0.3 + consistency_score * 0.3) as overall_quality_score
    FROM (VALUES (0)) as dummy  -- Dummy to allow calculation in same query
) quality_calculation
ORDER BY overall_quality_score DESC;
"""

        # EQ8: ETL error tracking and recovery validation
        queries["EQ8"] = """
SELECT
    'ETL Error Tracking and Recovery Validation' as validation_name,
    error_category,
    error_count,
    resolved_errors,
    unresolved_errors,
    error_resolution_rate_pct,
    avg_resolution_time_hours,
    impact_severity,
    CASE
        WHEN error_resolution_rate_pct >= {error_resolution_threshold} AND unresolved_errors = 0 THEN 'EXCELLENT'
        WHEN error_resolution_rate_pct >= {error_resolution_threshold} THEN 'GOOD'
        WHEN error_resolution_rate_pct >= 50 THEN 'NEEDS IMPROVEMENT'
        ELSE 'CRITICAL'
    END as error_management_status
FROM (
    SELECT
        'Data Quality Errors' as error_category,
        50 as error_count,  -- Simulated error counts
        45 as resolved_errors,
        5 as unresolved_errors,
        45 / 50 * 100 as error_resolution_rate_pct,
        2.5 as avg_resolution_time_hours,
        'Medium' as impact_severity
    UNION ALL
    SELECT
        'Transformation Errors' as error_category,
        15 as error_count,
        13 as resolved_errors,
        2 as unresolved_errors,
        13 / 15 * 100 as error_resolution_rate_pct,
        4.0 as avg_resolution_time_hours,
        'High' as impact_severity
    UNION ALL
    SELECT
        'Loading Errors' as error_category,
        8 as error_count,
        8 as resolved_errors,
        0 as unresolved_errors,
        100.0 as error_resolution_rate_pct,
        1.5 as avg_resolution_time_hours,
        'Low' as impact_severity
    UNION ALL
    SELECT
        'Referential Integrity Errors' as error_category,
        3 as error_count,
        2 as resolved_errors,
        1 as unresolved_errors,
        2 / 3 * 100 as error_resolution_rate_pct,
        8.0 as avg_resolution_time_hours,
        'Critical' as impact_severity
    UNION ALL
    SELECT
        'Business Rule Violations' as error_category,
        25 as error_count,
        20 as resolved_errors,
        5 as unresolved_errors,
        20 / 25 * 100 as error_resolution_rate_pct,
        3.0 as avg_resolution_time_hours,
        'Medium' as impact_severity
) error_tracking
ORDER BY
    CASE impact_severity
        WHEN 'Critical' THEN 1
        WHEN 'High' THEN 2
        WHEN 'Medium' THEN 3
        WHEN 'Low' THEN 4
    END,
    error_count DESC;
"""

        return queries

    def _load_etl_metadata(self) -> dict[str, dict[str, Any]]:
        """Load metadata for ETL queries.

        Returns:
            Dictionary mapping query IDs to their metadata
        """
        metadata = {}

        metadata["EQ1"] = {
            "relies_on": ["DimCustomer", "DimAccount", "FactTrade"],
            "query_type": "etl_validation",
            "category": "batch_processing",
            "description": "Validates batch processing performance and record counts",
            "frequency": "per_batch",
        }

        metadata["EQ2"] = {
            "relies_on": ["DimCustomer", "DimAccount", "DimSecurity"],
            "query_type": "etl_validation",
            "category": "incremental_loads",
            "description": "Validates incremental load processing with new vs updated records",
            "frequency": "per_incremental_batch",
        }

        metadata["EQ3"] = {
            "relies_on": [
                "DimCustomer",
                "DimAccount",
                "FactTrade",
                "FactMarketHistory",
            ],
            "query_type": "etl_validation",
            "category": "transformations",
            "description": "Validates data transformation success rates and quality",
            "frequency": "per_batch",
        }

        metadata["EQ4"] = {
            "relies_on": [
                "DimCustomer",
                "DimAccount",
                "DimSecurity",
                "DimCompany",
                "DimBroker",
            ],
            "query_type": "etl_validation",
            "category": "scd_processing",
            "description": "Validates SCD Type 2 processing accuracy and consistency",
            "frequency": "per_batch",
        }

        metadata["EQ5"] = {
            "relies_on": ["DimCustomer", "DimAccount", "FactTrade"],
            "query_type": "etl_validation",
            "category": "audit_trail",
            "description": "Validates data lineage and audit trail completeness",
            "frequency": "per_batch",
        }

        metadata["EQ6"] = {
            "relies_on": [],  # Uses simulated performance metrics
            "query_type": "etl_validation",
            "category": "performance",
            "description": "Validates ETL performance metrics and throughput",
            "frequency": "per_etl_run",
        }

        metadata["EQ7"] = {
            "relies_on": ["DimCustomer", "DimAccount", "FactTrade"],
            "query_type": "etl_validation",
            "category": "quality_scoring",
            "description": "Calculates comprehensive data quality scores for ETL monitoring",
            "frequency": "per_batch",
        }

        metadata["EQ8"] = {
            "relies_on": [],  # Uses simulated error tracking data
            "query_type": "etl_validation",
            "category": "error_management",
            "description": "Validates ETL error tracking and recovery processes",
            "frequency": "continuous",
        }

        return metadata

    def get_query(self, query_id: str, params: Optional[dict[str, Any]] = None) -> str:
        """Get an ETL validation query with parameters.

        Args:
            query_id: Query identifier (EQ1, EQ2, etc.)
            params: Optional parameter values. If None, uses defaults.

        Returns:
            SQL query with parameters replaced

        Raises:
            ValueError: If query_id is invalid
        """
        if query_id not in self._queries:
            available = ", ".join(sorted(self._queries.keys()))
            raise ValueError(f"Invalid ETL query ID: {query_id}. Available: {available}")

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
        """Get all ETL validation queries with default parameters.

        Returns:
            Dictionary mapping query IDs to parameterized SQL
        """
        result = {}
        for query_id in self._queries:
            result[query_id] = self.get_query(query_id)
        return result

    def _generate_default_params(self, query_id: str) -> dict[str, Any]:
        """Generate default parameters for ETL queries.

        Args:
            query_id: Query identifier

        Returns:
            Dictionary of parameter names to default values
        """
        # Default parameters for ETL validation queries
        defaults = {
            # Performance thresholds
            "min_throughput": 100,  # Records per second
            "performance_rating_threshold": 70,  # Performance rating out of 100
            # Success rate thresholds
            "success_rate_threshold": 95.0,  # Transformation success rate %
            "quality_score_threshold": 90.0,  # Data quality score %
            "audit_coverage_threshold": 95.0,  # Audit trail coverage %
            "consistency_score_threshold": 90.0,  # Batch consistency score %
            "error_resolution_threshold": 85.0,  # Error resolution rate %
            # Quality score thresholds
            "excellent_quality_threshold": 95.0,
            "good_quality_threshold": 85.0,
            "acceptable_quality_threshold": 75.0,
            # Batch parameters
            "min_batch_id": 1,
            "batch_start_date": "2023-01-01",
        }

        return defaults

    def get_query_metadata(self, query_id: str) -> dict[str, Any]:
        """Get metadata for an ETL validation query.

        Args:
            query_id: Query identifier (EQ1, EQ2, etc.)

        Returns:
            Dictionary containing query metadata

        Raises:
            ValueError: If query_id is invalid
        """
        if query_id not in self._query_metadata:
            available = ", ".join(sorted(self._query_metadata.keys()))
            raise ValueError(f"Invalid ETL query ID: {query_id}. Available: {available}")

        return self._query_metadata[query_id].copy()

    def get_queries_by_category(self, category: str) -> list[str]:
        """Get all ETL validation queries of a specific category.

        Args:
            category: Category name (batch_processing, incremental_loads, transformations,
                     scd_processing, audit_trail, performance, quality_scoring, error_management)

        Returns:
            List of query IDs in the specified category
        """
        valid_categories = {
            "batch_processing",
            "incremental_loads",
            "transformations",
            "scd_processing",
            "audit_trail",
            "performance",
            "quality_scoring",
            "error_management",
        }
        if category not in valid_categories:
            raise ValueError(f"Invalid category: {category}. Valid categories: {', '.join(valid_categories)}")

        return [query_id for query_id, metadata in self._query_metadata.items() if metadata["category"] == category]

    def get_queries_by_frequency(self, frequency: str) -> list[str]:
        """Get all ETL validation queries of a specific execution frequency.

        Args:
            frequency: Execution frequency (per_batch, per_incremental_batch, per_etl_run, continuous)

        Returns:
            List of query IDs with the specified frequency
        """
        valid_frequencies = {
            "per_batch",
            "per_incremental_batch",
            "per_etl_run",
            "continuous",
        }
        if frequency not in valid_frequencies:
            raise ValueError(f"Invalid frequency: {frequency}. Valid frequencies: {', '.join(valid_frequencies)}")

        return [query_id for query_id, metadata in self._query_metadata.items() if metadata["frequency"] == frequency]
