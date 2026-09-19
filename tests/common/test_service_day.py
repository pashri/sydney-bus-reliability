"""Tests for Sydney service-day arithmetic."""

from datetime import UTC, date, datetime, timedelta

import pytest

from common.service_day import (
    SYDNEY,
    merge_window,
    parse_gtfs_time,
    scheduled_instant,
    service_date_for,
)


def test_parse_gtfs_time_ordinary() -> None:
    """A normal clock time parses to its offset from midnight."""
    assert parse_gtfs_time(value='07:30:00') == timedelta(
        hours=7, minutes=30,
    )


def test_parse_gtfs_time_past_midnight() -> None:
    """Hour 25 is 01:00 the following calendar day."""
    assert parse_gtfs_time(value='25:15:30') == timedelta(
        hours=25, minutes=15, seconds=30,
    )


def test_parse_gtfs_time_maximum_observed_hour() -> None:
    """Hour 30 was measured in the real bundle and must parse."""
    assert parse_gtfs_time(value='30:00:00') == timedelta(hours=30)


def test_parse_gtfs_time_rejects_rubbish() -> None:
    """A malformed time raises rather than returning a default."""
    with pytest.raises(ValueError):
        parse_gtfs_time(value='not-a-time')


def test_scheduled_instant_aest() -> None:
    """Before 4 October, Sydney is UTC+10."""
    result = scheduled_instant(
        start_date='20260917', gtfs_time='07:30:00',
    )
    assert result == datetime(2026, 9, 16, 21, 30, tzinfo=UTC)


def test_scheduled_instant_aedt() -> None:
    """After 4 October, Sydney is UTC+11 and the offset must follow."""
    result = scheduled_instant(
        start_date='20261006', gtfs_time='07:30:00',
    )
    assert result == datetime(2026, 10, 5, 20, 30, tzinfo=UTC)


def test_scheduled_instant_past_midnight() -> None:
    """A 25:15 trip on the 17th is 01:15 Sydney on the 18th."""
    result = scheduled_instant(
        start_date='20260917', gtfs_time='25:15:00',
    )
    assert result == datetime(2026, 9, 17, 15, 15, tzinfo=UTC)


def test_service_date_for_before_sydney_midnight() -> None:
    """An instant inside the Sydney day maps to that date."""
    instant = datetime(2026, 9, 17, 2, 0, tzinfo=UTC)
    assert service_date_for(instant=instant) == date(2026, 9, 17)


def test_merge_window_spans_the_sydney_day_plus_margin() -> None:
    """The window starts an hour early and runs six hours past."""
    start, end = merge_window(service_date=date(2026, 9, 17))
    assert start == datetime(2026, 9, 16, 13, 0, tzinfo=UTC)
    assert end == datetime(2026, 9, 17, 21, 0, tzinfo=UTC)


def test_scheduled_instant_spans_dst_gap_keeps_wall_clock() -> None:
    """A 30:00 trip on 3 October is 06:00 wall clock the next day.

    Sydney springs forward at 02:00 on 4 October, so that service day
    is 23 real hours long. Agencies generate GTFS from wall-clock
    scheduling systems, so 30:00 means the timetable says 06:00 - not
    30 elapsed hours, which would be 07:00.
    """
    result = scheduled_instant(
        start_date='20261003', gtfs_time='30:00:00',
    )
    assert result == datetime(2026, 10, 3, 19, 0, tzinfo=UTC)
    assert result.astimezone(SYDNEY).hour == 6


def test_scheduled_instant_before_dst_gap_stays_aest() -> None:
    """A 01:30 trip on 4 October is before the 02:00 jump."""
    result = scheduled_instant(
        start_date='20261004', gtfs_time='01:30:00',
    )
    assert result == datetime(2026, 10, 3, 15, 30, tzinfo=UTC)


def test_scheduled_instant_after_dst_gap_is_aedt() -> None:
    """An ordinary 07:30 trip on 4 October is 07:30, not 08:30.

    Anchoring elapsed seconds at local midnight instead would move
    every daytime service on this date forward by an hour.
    """
    result = scheduled_instant(
        start_date='20261004', gtfs_time='07:30:00',
    )
    assert result == datetime(2026, 10, 3, 20, 30, tzinfo=UTC)
    assert result.astimezone(SYDNEY).hour == 7


def test_merge_window_covers_the_short_dst_day() -> None:
    """4 October is 23 hours long; the window must still cover it."""
    start, end = merge_window(service_date=date(2026, 10, 4))
    assert start == datetime(2026, 10, 3, 13, 0, tzinfo=UTC)
    assert end == datetime(2026, 10, 4, 20, 0, tzinfo=UTC)


def test_merge_window_trailing_margin_covers_hour_30() -> None:
    """Hour 30 on the day before the shift must fall inside the window.

    The real trailing coverage shrinks by an hour on that date, so
    this is the case where a too-small margin would silently drop the
    night's last services.
    """
    start, end = merge_window(service_date=date(2026, 10, 3))
    latest = scheduled_instant(
        start_date='20261003', gtfs_time='30:00:00',
    )
    assert start <= latest <= end
