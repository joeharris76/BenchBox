"""Coverage-focused tests for TPC-Havoc validation utilities."""

from __future__ import annotations

import pytest

from benchbox.core.tpchavoc.validation import ResultValidator, ValidationError, ValidationReport

pytestmark = [pytest.mark.fast, pytest.mark.unit]


def test_validate_results_exact_accepts_reordered_rows() -> None:
    validator = ResultValidator()

    original = [("A", 1.0), ("B", 2.0)]
    variant = [("B", 2.0), ("A", 1.0)]

    assert validator.validate_results_exact(original, variant, query_id=3, variant_id=2)


@pytest.mark.parametrize(
    ("original", "variant", "message"),
    [
        ([(1,)], [(1,), (2,)], "Row count mismatch"),
        ([(1, 2)], [(1,)], "Column count mismatch"),
        ([(1, "x")], [(2, "x")], "Value mismatch"),
    ],
)
def test_validate_results_exact_failure_modes(
    original: list[tuple[object, ...]],
    variant: list[tuple[object, ...]],
    message: str,
) -> None:
    validator = ResultValidator()

    with pytest.raises(ValidationError, match=message):
        validator.validate_results_exact(original, variant, query_id=4, variant_id=1)


def test_numeric_and_string_value_comparison_behavior() -> None:
    validator = ResultValidator(tolerance=1e-6)

    assert validator._values_equal("  abc ", "abc")
    assert validator._values_equal(1.0000001, 1.0000002)
    assert validator._values_equal(None, None)
    assert not validator._values_equal(None, 1)
    assert not validator._values_equal("x", "y")


def test_validate_results_checksum_mismatch_raises() -> None:
    validator = ResultValidator()

    with pytest.raises(ValidationError, match="Checksum mismatch"):
        validator.validate_results_checksum([(1, "a")], [(2, "b")], query_id=10, variant_id=9)


def test_validate_aggregation_results_uses_tolerance_for_numeric_columns() -> None:
    validator = ResultValidator(tolerance=1e-3)

    original = [("x", 100.0, 2)]
    variant = [("x", 100.00005, 2)]

    assert validator.validate_aggregation_results(
        original,
        variant,
        query_id=1,
        variant_id=3,
        aggregation_columns=[1],
    )


def test_validate_query1_results_delegates_aggregation_columns(monkeypatch: pytest.MonkeyPatch) -> None:
    validator = ResultValidator()
    captured: dict[str, object] = {}

    def fake_validate(
        original_results: list[tuple[object, ...]],
        variant_results: list[tuple[object, ...]],
        query_id: int,
        variant_id: int,
        aggregation_columns: list[int] | None = None,
    ) -> bool:
        captured["query_id"] = query_id
        captured["variant_id"] = variant_id
        captured["aggregation_columns"] = aggregation_columns
        return True

    monkeypatch.setattr(validator, "validate_aggregation_results", fake_validate)

    assert validator.validate_query1_results([("R", "F", 1.0)], [("R", "F", 1.0)], variant_id=4)
    assert captured == {
        "query_id": 1,
        "variant_id": 4,
        "aggregation_columns": [2, 3, 4, 5, 6, 7, 8],
    }


def test_calculate_checksum_represents_null_values() -> None:
    validator = ResultValidator()

    checksum1 = validator._calculate_checksum([(1, None), (2, "x")])
    checksum2 = validator._calculate_checksum([(2, "x"), (1, None)])

    assert checksum1 == checksum2


def test_validation_report_summaries_and_text_report() -> None:
    report = ValidationReport()
    report.add_validation_result(1, 1, success=True, execution_time_original=10.0, execution_time_variant=9.0)
    report.add_validation_result(2, 1, success=False, error_message="mismatch")

    summary = report.get_summary()
    perf = report.get_performance_summary()
    text = report.generate_report()

    assert summary["total_tests"] == 2
    assert summary["successful_tests"] == 1
    assert summary["failed_queries"] == ["Q2.1"]
    assert perf["variants_faster"] == 1
    assert "TPC-Havoc Validation Report" in text
    assert "Q2.1" in text


def test_validation_report_no_performance_data_message() -> None:
    report = ValidationReport()
    report.add_validation_result(3, 1, success=True)

    assert report.get_performance_summary() == {"message": "No performance data available"}
