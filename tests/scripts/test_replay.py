"""Tests for the replay planner."""

import io
import json
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pytest

from common.service_day import merge_window
from scripts.replay import (
    ReplayLog,
    failure_of,
    hours_to_compact,
    last_complete_day,
    parse_raw_hour,
)


def test_parse_raw_hour_reads_the_partition_path() -> None:
    """A raw hour prefix names its UTC hour."""
    assert parse_raw_hour(
        prefix='raw/tripupdates/dt=2026-09-16/hour=16/',
    ) == datetime(2026, 9, 16, 16, tzinfo=UTC)


def test_hours_to_compact_cover_every_merge_window() -> None:
    """Every raw hour inside the replayed days' windows is compacted."""
    start, _ = merge_window(service_date=date(2026, 9, 17))
    _, end = merge_window(service_date=date(2026, 9, 18))
    available = {
        start - timedelta(hours=1),
        start,
        end - timedelta(hours=1),
        end,
    }
    assert hours_to_compact(
        available=available,
        first=date(2026, 9, 17),
        last=date(2026, 9, 18),
        now=end + timedelta(days=1),
    ) == [start, end - timedelta(hours=1)]


def test_last_complete_day_is_the_latest_closed_window() -> None:
    """A day is complete once its whole merge window has passed."""
    _, end = merge_window(service_date=date(2026, 9, 22))
    assert last_complete_day(now=end) == date(2026, 9, 22)
    assert last_complete_day(
        now=end - timedelta(seconds=1),
    ) == date(2026, 9, 21)


def test_failure_of_reads_a_function_error() -> None:
    """A handled Lambda error is reported with its message."""
    response = {
        'FunctionError': 'Unhandled',
        'Payload': io.BytesIO(json.dumps({
            'errorType': 'RuntimeError', 'errorMessage': 'no partials',
        }).encode()),
    }
    assert failure_of(response=response) == 'RuntimeError: no partials'


def test_failure_of_is_none_on_success() -> None:
    """A successful invocation has no failure."""
    response = {'Payload': io.BytesIO(b'{"rows_out": 3}')}
    assert failure_of(response=response) is None


def test_replay_log_remembers_finished_steps(tmp_path: Path) -> None:
    """A resumed replay skips what an earlier run finished."""
    path = tmp_path / 'replay.jsonl'
    ReplayLog(path=path).record(step='compact 2026-09-16T16')
    ReplayLog(path=path).record(step='merge 2026-09-17')
    assert ReplayLog(path=path).done() == {
        'compact 2026-09-16T16', 'merge 2026-09-17',
    }


def test_replay_log_without_a_file_has_nothing_done(tmp_path: Path) -> None:
    """A first run starts from nothing."""
    assert not ReplayLog(path=tmp_path / 'missing.jsonl').done()


@pytest.mark.parametrize('first', [date(2026, 9, 23), date(2026, 9, 24)])
def test_hours_to_compact_rejects_an_open_day(first: date) -> None:
    """A day whose window has not closed cannot be replayed yet."""
    _, end = merge_window(service_date=date(2026, 9, 22))
    with pytest.raises(ValueError, match='not complete'):
        hours_to_compact(
            available=set(), first=first, last=first, now=end,
        )
