"""Regression tests for timing collector clock usage."""

from __future__ import annotations

from datetime import datetime, timezone

from benchbox.core.results.timing import TimingCollector


def test_time_query_records_wall_clock_timestamp() -> None:
    collector = TimingCollector()

    before = datetime.now(timezone.utc)
    with collector.time_query("Q1"):
        pass
    after = datetime.now(timezone.utc)

    completed = collector.get_completed_timings()
    assert len(completed) == 1

    timestamp = completed[0].timestamp
    assert timestamp.year >= 2000
    assert before <= timestamp <= after
