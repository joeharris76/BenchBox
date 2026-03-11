"""Tests for QueryResult timing schema contract."""

from __future__ import annotations

import pytest

from benchbox.core.schemas import QueryResult

pytestmark = [
    pytest.mark.unit,
    pytest.mark.fast,
]


def test_query_result_accepts_canonical_execution_time_seconds() -> None:
    result = QueryResult(
        query_id="Q1",
        query_name="Query 1",
        sql_text="SELECT 1",
        execution_time_seconds=1.25,
        rows_returned=1,
        status="SUCCESS",
    )

    assert isinstance(result.execution_time_seconds, float)
    assert result.execution_time_seconds == 1.25
    assert result.execution_time_ms == 1250.0


def test_query_result_derives_seconds_from_legacy_ms() -> None:
    result = QueryResult(
        query_id="Q1",
        query_name="Query 1",
        sql_text="SELECT 1",
        execution_time_ms=1500,
        rows_returned=1,
        status="SUCCESS",
    )

    assert result.execution_time_seconds == 1.5
    assert result.execution_time_ms == 1500.0
