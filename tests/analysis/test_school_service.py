"""Tests for deriving school services from timetable cancellations."""

from datetime import date

import pytest

from analysis.calendar_exclusion import (
    Exclusion,
    ExclusionType,
    load_exclusions,
)
from analysis.school_service import (
    CalendarException,
    TripRoute,
    cancelled_services,
    contiguous_runs,
    covered_span,
    holiday_blocks,
    parse_gtfs_date,
    school_holiday_weekdays,
    school_routes,
    school_services,
)

SPRING = (
    date(2026, 9, 28),
    date(2026, 9, 29),
    date(2026, 10, 1),
)


def _exclusion(
    day: date,
    exclusion_type: ExclusionType = ExclusionType.SCHOOL_HOLIDAY,
) -> Exclusion:
    """Build one exclusion record."""
    return Exclusion(
        date=day,
        exclusion_type=exclusion_type,
        reason='test',
        source='test',
    )


def _removals(
    service_id: str,
    days: tuple[date, ...],
) -> list[CalendarException]:
    """Build removal exceptions for one service."""
    return [
        CalendarException(
            service_id=service_id,
            date=day,
            exception_type='2',
        )
        for day in days
    ]


def test_parse_gtfs_date() -> None:
    assert parse_gtfs_date(value='20261005') == date(2026, 10, 5)


def test_parse_gtfs_date_rejects_rubbish() -> None:
    with pytest.raises(ValueError):
        parse_gtfs_date(value='2026-10-05')


def test_school_holiday_weekdays_drops_weekends() -> None:
    days = school_holiday_weekdays(
        exclusions=[
            _exclusion(date(2026, 10, 2)),
            _exclusion(date(2026, 10, 3)),
            _exclusion(date(2026, 10, 4)),
        ],
    )
    assert days == frozenset({date(2026, 10, 2)})


def test_school_holiday_weekdays_drops_public_holidays() -> None:
    days = school_holiday_weekdays(
        exclusions=[
            _exclusion(date(2026, 10, 5)),
            _exclusion(date(2026, 10, 5), ExclusionType.PUBLIC_HOLIDAY),
            _exclusion(date(2026, 10, 6)),
        ],
    )
    assert days == frozenset({date(2026, 10, 6)})


def test_school_holiday_weekdays_ignores_other_types() -> None:
    days = school_holiday_weekdays(
        exclusions=[
            _exclusion(
                date(2026, 10, 12),
                ExclusionType.SCHOOL_DEVELOPMENT_DAY,
            ),
        ],
    )
    assert days == frozenset()


def test_cancelled_services_needs_every_date() -> None:
    exceptions = [
        *_removals('all', SPRING),
        *_removals('some', SPRING[:2]),
    ]
    found = cancelled_services(
        exceptions=exceptions,
        on_dates=frozenset(SPRING),
    )
    assert found == frozenset({'all'})


def test_cancelled_services_ignores_additions() -> None:
    exceptions = [
        CalendarException(
            service_id='added',
            date=day,
            exception_type='1',
        )
        for day in SPRING
    ]
    found = cancelled_services(
        exceptions=exceptions,
        on_dates=frozenset(SPRING),
    )
    assert found == frozenset()


def test_cancelled_services_ignores_dates_outside_the_window() -> None:
    exceptions = _removals('elsewhere', (date(2026, 12, 25),))
    found = cancelled_services(
        exceptions=exceptions,
        on_dates=frozenset(SPRING),
    )
    assert found == frozenset()


def test_cancelled_services_without_dates_finds_nothing() -> None:
    found = cancelled_services(
        exceptions=_removals('all', SPRING),
        on_dates=frozenset(),
    )
    assert found == frozenset()


def test_school_services_uses_the_real_seed() -> None:
    exclusions = load_exclusions(year=2026)
    dates = school_holiday_weekdays(exclusions=exclusions)
    found = school_services(
        exceptions=_removals('school', tuple(dates)),
        exclusions=exclusions,
    )
    assert found == frozenset({'school'})


def test_contiguous_runs_splits_on_a_gap() -> None:
    runs = contiguous_runs(
        days=[
            date(2026, 10, 1),
            date(2026, 10, 2),
            date(2026, 10, 6),
        ],
    )
    assert runs == [
        [date(2026, 10, 1), date(2026, 10, 2)],
        [date(2026, 10, 6)],
    ]


def test_contiguous_runs_of_nothing() -> None:
    assert contiguous_runs(days=[]) == []


def test_holiday_blocks_finds_each_break() -> None:
    blocks = holiday_blocks(exclusions=load_exclusions(year=2026))
    assert len(blocks) == 5
    spring = [b for b in blocks if date(2026, 9, 28) in b]
    assert len(spring) == 1
    assert max(spring[0]) == date(2026, 10, 9)
    assert date(2026, 10, 5) not in spring[0]


def test_holiday_blocks_keeps_a_break_whole_across_its_weekends() -> None:
    blocks = holiday_blocks(
        exclusions=[
            _exclusion(date(2026, 10, 2)),
            _exclusion(date(2026, 10, 3)),
            _exclusion(date(2026, 10, 4)),
            _exclusion(date(2026, 10, 5)),
            _exclusion(date(2026, 10, 6)),
        ],
    )
    assert blocks == [frozenset({date(2026, 10, 2), date(2026, 10, 5),
                                 date(2026, 10, 6)})]


def test_covered_span_of_nothing() -> None:
    assert covered_span(exceptions=[]) is None


def test_covered_span_bounds_the_dates() -> None:
    assert covered_span(exceptions=_removals('s', SPRING)) == (
        date(2026, 9, 28),
        date(2026, 10, 1),
    )


def test_school_services_clips_to_the_bundle_span() -> None:
    exclusions = load_exclusions(year=2026)
    spring_only = _removals(
        'school',
        (date(2026, 9, 28), date(2026, 9, 29), date(2026, 9, 30),
         date(2026, 10, 1), date(2026, 10, 2), date(2026, 10, 6),
         date(2026, 10, 7), date(2026, 10, 8), date(2026, 10, 9)),
    )
    found = school_services(exceptions=spring_only, exclusions=exclusions)
    assert found == frozenset({'school'})


def test_school_services_without_exceptions() -> None:
    found = school_services(
        exceptions=[],
        exclusions=load_exclusions(year=2026),
    )
    assert found == frozenset()


def test_school_services_ignores_a_labour_day_only_cancellation() -> None:
    exclusions = load_exclusions(year=2026)
    found = school_services(
        exceptions=_removals('labour_day_only', (date(2026, 10, 5),)),
        exclusions=exclusions,
    )
    assert found == frozenset()


def test_school_routes_needs_every_trip_on_a_school_service() -> None:
    trips = [
        TripRoute(route_id='S265', service_id='school'),
        TripRoute(route_id='S265', service_id='school'),
        TripRoute(route_id='753', service_id='school'),
        TripRoute(route_id='753', service_id='allyear'),
    ]
    found = school_routes(trips=trips, services=frozenset({'school'}))
    assert found == frozenset({'S265'})


def test_school_routes_without_school_services() -> None:
    trips = [TripRoute(route_id='400', service_id='allyear')]
    assert school_routes(trips=trips, services=frozenset()) == frozenset()


def test_school_routes_without_trips() -> None:
    assert school_routes(trips=[], services=frozenset({'x'})) == frozenset()
