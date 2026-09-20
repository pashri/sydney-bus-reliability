"""Tests for the curation half of the checker."""

from datetime import date
from typing import Any

from checker.curation import (
    expected_hour_partitions,
    hours_in_day,
    summarize_curation,
    summarize_job,
)
from common.types_ import CurationJob

DAY = date(2026, 9, 15)
DST_FORWARD = date(2026, 10, 4)
DST_BACK = date(2027, 4, 4)


def _record(
    *,
    job: str = CurationJob.COMPACTOR.value,
    partition: str,
    rows_in: int = 100,
    rows_out: int = 100,
    unjoined_trip_ids: int = 0,
    peak_rss_mb: int = 200,
    error: str | None = None,
) -> dict[str, Any]:
    """Build one curation audit row."""
    return {
        'job': job,
        'invocation_id': f'id-{partition}',
        'started_at_utc': '2026-09-15T01:10:00+00:00',
        'finished_at_utc': '2026-09-15T01:10:30+00:00',
        'partition': partition,
        'objects_expected': 120,
        'objects_read': 120,
        'rows_in': rows_in,
        'rows_out': rows_out,
        'dupes_collapsed': 0,
        'dupes_differing_position': 0,
        'unjoined_route_ids': 0,
        'unjoined_trip_ids': unjoined_trip_ids,
        'unjoined_stop_ids': 0,
        'peak_rss_mb': peak_rss_mb,
        'error': error,
    }


def test_hours_in_day_ordinary_day_is_twenty_four() -> None:
    assert hours_in_day(day=DAY) == 24


def test_hours_in_day_shortened_by_clocks_going_forward() -> None:
    """Sydney loses an hour, so a fixed 24 would invent a gap."""
    assert hours_in_day(day=DST_FORWARD) == 23


def test_hours_in_day_lengthened_by_clocks_going_back() -> None:
    assert hours_in_day(day=DST_BACK) == 25


def test_expected_hour_partitions_matches_day_length() -> None:
    for day in (DAY, DST_FORWARD, DST_BACK):
        partitions = expected_hour_partitions(day=day)
        assert len(partitions) == hours_in_day(day=day)
        assert len(set(partitions)) == len(partitions)


def test_expected_hour_partitions_are_consecutive_utc_hours() -> None:
    """Labels step one UTC hour at a time across a transition."""
    partitions = expected_hour_partitions(day=DST_FORWARD)
    assert partitions[0] == '2026-10-03T14'
    assert partitions[-1] == '2026-10-04T12'


def test_summarize_job_counts_every_expected_run() -> None:
    expected = expected_hour_partitions(day=DAY)
    records = [_record(partition=one) for one in expected]
    summary = summarize_job(records, expected=expected)
    assert summary.runs_expected == 24
    assert summary.runs_seen == 24
    assert summary.missing_partitions == []


def test_summarize_job_reports_a_missing_hour() -> None:
    """A skipped hour is invisible to an invocation-count alarm."""
    expected = expected_hour_partitions(day=DAY)
    records = [_record(partition=one) for one in expected[:-1]]
    summary = summarize_job(records, expected=expected)
    assert summary.runs_seen == 23
    assert summary.missing_partitions == [expected[-1]]


def test_summarize_job_ignores_records_from_other_days() -> None:
    """Neighbouring UTC partitions are read but must not be counted."""
    expected = expected_hour_partitions(day=DAY)
    records = [_record(partition=one) for one in expected]
    records.append(_record(partition='2026-09-16T23', rows_in=999))
    summary = summarize_job(records, expected=expected)
    assert summary.runs_seen == 24
    assert summary.totals['rows_in'] == 100 * 24


def test_summarize_job_sums_counters_across_runs() -> None:
    expected = expected_hour_partitions(day=DAY)
    records = [
        _record(partition=one, unjoined_trip_ids=2) for one in expected
    ]
    summary = summarize_job(records, expected=expected)
    assert summary.totals['unjoined_trip_ids'] == 48


def test_summarize_job_counts_runs_that_recorded_an_error() -> None:
    expected = expected_hour_partitions(day=DAY)
    records = [_record(partition=one) for one in expected]
    records[0] = _record(partition=expected[0], error='boom')
    summary = summarize_job(records, expected=expected)
    assert summary.errors == 1


def test_summarize_job_takes_the_largest_peak_memory() -> None:
    expected = expected_hour_partitions(day=DAY)
    records = [_record(partition=one) for one in expected]
    records[3] = _record(partition=expected[3], peak_rss_mb=901)
    summary = summarize_job(records, expected=expected)
    assert summary.peak_rss_mb == 901


def test_summarize_job_with_no_records_reports_every_hour_missing() -> (
    None
):
    expected = expected_hour_partitions(day=DAY)
    summary = summarize_job([], expected=expected)
    assert summary.runs_seen == 0
    assert summary.missing_partitions == expected
    assert summary.peak_rss_mb == 0


def test_summarize_curation_merger_owes_one_run_a_day() -> None:
    records = [
        _record(job=CurationJob.MERGER.value, partition='2026-09-15'),
    ]
    summary = summarize_curation(records, day=DAY, checks=[])
    merger = summary.jobs[CurationJob.MERGER.value]
    assert merger.runs_expected == 1
    assert merger.runs_seen == 1


def test_summarize_curation_compactor_owes_one_run_an_hour() -> None:
    summary = summarize_curation([], day=DAY, checks=[])
    compactor = summary.jobs[CurationJob.COMPACTOR.value]
    assert compactor.runs_expected == 24


def test_summarize_curation_shortens_expectations_across_dst() -> None:
    """The compactor owes 23 runs on the day the clocks go forward."""
    summary = summarize_curation([], day=DST_FORWARD, checks=[])
    compactor = summary.jobs[CurationJob.COMPACTOR.value]
    assert summary.hours_in_day == 23
    assert compactor.runs_expected == 23


def test_summarize_curation_reports_a_missing_schedule_check() -> None:
    summary = summarize_curation([], day=DAY, checks=[])
    assert summary.schedule.checks_expected == 1
    assert summary.schedule.checks_seen == 0
    assert summary.schedule.changed is False


def test_summarize_curation_reports_a_changed_timetable() -> None:
    checks = [{'changed': False}, {'changed': True}]
    summary = summarize_curation([], day=DAY, checks=checks)
    assert summary.schedule.checks_seen == 2
    assert summary.schedule.changed is True
