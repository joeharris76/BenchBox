from __future__ import annotations

import pandas as pd
import pytest

from benchbox.core.tpcdi.etl.validation import (
    BasicDataValidator,
    TPCDITableValidators,
    quick_cross_table_validation,
    quick_validate_data,
    validate_customer_tier,
    validate_date_format,
    validate_foreign_key,
    validate_gender,
    validate_not_null,
    validate_numeric_range,
    validate_security_symbol,
    validate_trade_type,
    validate_unique_values,
)

pytestmark = [pytest.mark.fast, pytest.mark.unit]


def test_core_validation_helpers_cover_missing_column_paths():
    data = pd.DataFrame({"id": [1, 2]})
    ref = pd.DataFrame({"other": [1]})

    not_null = validate_not_null(data, ["missing"], "nn_missing")
    unique = validate_unique_values(data, ["missing"], "uq_missing")
    ranged = validate_numeric_range(data, "missing", min_value=0, max_value=20, rule_name="range_missing")
    fk_missing_col = validate_foreign_key(data, "missing_fk", ref, "other", "fk_missing_col")
    fk_missing_ref = validate_foreign_key(data, "id", ref, "missing_ref", "fk_missing_ref")
    date_missing = validate_date_format(data, "missing_date", "date_missing")

    assert not not_null.passed and not_null.violation_count == 1
    assert not unique.passed and unique.violation_count == 1
    assert not ranged.passed and ranged.violation_count == 1
    assert not fk_missing_col.passed and fk_missing_col.violation_count == 1
    assert not fk_missing_ref.passed and fk_missing_ref.violation_count == 1
    assert not date_missing.passed and date_missing.violation_count == 1


def test_basic_validator_routes_tables_and_tracks_history():
    validator = BasicDataValidator()
    customer = pd.DataFrame({"unrelated": [1]})
    account = pd.DataFrame({"other": [1]})
    security = pd.DataFrame({"x": [1]})
    trade = pd.DataFrame({"y": [1]})
    company = pd.DataFrame({"z": [1]})
    generic = pd.DataFrame({"a": [None, None], "b": [None, 1]})

    results = []
    results.extend(validator.validate_table("DimCustomer", customer))
    results.extend(validator.validate_table("DimAccount", account))
    results.extend(validator.validate_table("DimSecurity", security))
    results.extend(validator.validate_table("FactTrade", trade))
    results.extend(validator.validate_table("DimCompany", company))
    results.extend(validator.validate_table("Other", generic))

    stats = validator.get_validation_statistics()
    assert len(results) >= 1
    assert stats["total_rules"] == len(validator.validation_history)
    assert stats["status"] == "completed"


def test_cross_table_and_summary_paths():
    validator = BasicDataValidator()
    batch = {
        "DimCustomer": pd.DataFrame({"x": [1]}),
        "DimAccount": pd.DataFrame({"y": [1]}),
        "DimSecurity": pd.DataFrame({"z": [1]}),
        "DimCompany": pd.DataFrame({"w": [1]}),
        "FactTrade": pd.DataFrame({"q": [1]}),
    }
    cross_results = validator.validate_cross_table_references(batch)
    summary = validator.generate_validation_summary(cross_results)
    empty_summary = validator.generate_validation_summary([])

    assert len(cross_results) == 4
    assert summary["failed_rules_count"] == 4
    assert empty_summary["status"] == "no_results"


def test_tpcdi_business_rules_and_quick_wrappers_missing_columns():
    customer = pd.DataFrame({"other": [1]})
    trade = pd.DataFrame({"other": [1]})
    security = pd.DataFrame({"other": [1]})

    assert validate_customer_tier(customer).passed
    assert validate_gender(customer).passed
    assert validate_trade_type(trade).passed
    assert validate_security_symbol(security).passed

    c_results = TPCDITableValidators.validate_customer_data(customer)
    t_results = TPCDITableValidators.validate_trade_data(trade)
    s_results = TPCDITableValidators.validate_security_data(security)
    quick_table = quick_validate_data(customer, "DimCustomer")
    quick_cross = quick_cross_table_validation(
        {
            "DimCustomer": pd.DataFrame({"SK_CustomerID": [1]}),
            "DimAccount": pd.DataFrame({"SK_AccountID": [1], "SK_CustomerID": [1]}),
        }
    )

    assert len(c_results) == 2
    assert len(t_results) == 1
    assert len(s_results) == 1
    assert quick_table["status"] == "no_results"
    assert quick_cross["status"] == "completed"
