"""Identifying school bus services from the timetable's own cancellations.

The feed carries no flag for a school service. It does say which
services do not run on a given date, and school services are withdrawn
for the school holidays, so the services cancelled on every school
holiday weekday are the school services.

This is more accurate than reading route names. Names miss school runs
that name the school without saying so - ``Balgowlah Boys High`` - and
wrongly catch all-year public routes that happen to terminate at a
school, which keep running through the holidays.

Public holidays are left out of the reference dates. Service is
withdrawn on those for a different reason, and including them would
admit ordinary routes that simply do not run on a public holiday.

The classification holds for the bundle it was derived from. A service
pattern renumbered mid-year changes it, so recompute rather than
freezing the result.
"""

import calendar
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Final

from analysis.calendar_exclusion import Exclusion, ExclusionType

SERVICE_REMOVED: Final[str] = '2'
"""``exception_type`` marking a service as not running on a date."""


@dataclass(frozen=True, slots=True)
class CalendarException:
    """One single-day exception to a service pattern."""

    service_id: str
    date: date
    exception_type: str


@dataclass(frozen=True, slots=True)
class TripRoute:
    """The route and service pattern of one scheduled trip."""

    route_id: str
    service_id: str


def parse_gtfs_date(*, value: str) -> date:
    """Parse a GTFS ``YYYYMMDD`` date.

    Parameters
    ----------
    value : str
        Eight-digit date as the feed writes it.

    Returns
    -------
    date
        The parsed date.

    Raises
    ------
    ValueError
        If the value is not an eight-digit date.
    """
    return date(int(value[:4]), int(value[4:6]), int(value[6:]))


def school_holiday_weekdays(
    *,
    exclusions: Iterable[Exclusion],
) -> frozenset[date]:
    """Pick the reference dates a school service must be absent from.

    Parameters
    ----------
    exclusions : Iterable[Exclusion]
        Exclusion records for the year.

    Returns
    -------
    frozenset[date]
        School holiday weekdays that are not also public holidays.
    """
    records = list(exclusions)
    holidays = {
        item.date
        for item in records
        if item.exclusion_type is ExclusionType.PUBLIC_HOLIDAY
    }
    return frozenset(
        item.date
        for item in records
        if item.exclusion_type is ExclusionType.SCHOOL_HOLIDAY
        and item.date.weekday() < calendar.SATURDAY
        and item.date not in holidays
    )


def contiguous_runs(*, days: Iterable[date]) -> list[list[date]]:
    """Group dates into runs of consecutive days.

    Parameters
    ----------
    days : Iterable[date]
        Dates to group. Order and duplicates do not matter.

    Returns
    -------
    list[list[date]]
        Runs in ascending order, each itself ascending.
    """
    runs: list[list[date]] = []
    for day in sorted(set(days)):
        if runs and day - runs[-1][-1] == timedelta(days=1):
            runs[-1].append(day)
        else:
            runs.append([day])
    return runs


def holiday_blocks(
    *,
    exclusions: Iterable[Exclusion],
) -> list[frozenset[date]]:
    """Split the school holidays into separate breaks.

    Each break is treated on its own. Services are withdrawn for one
    break at a time, and the summer break coincides with a wholesale
    timetable change rather than a set of single-day cancellations, so
    requiring absence across every break at once finds nothing.

    Parameters
    ----------
    exclusions : Iterable[Exclusion]
        Exclusion records for the year.

    Returns
    -------
    list[frozenset[date]]
        One set of usable reference dates per break, earliest first.
        Breaks with no usable dates are dropped.
    """
    records = list(exclusions)
    usable = school_holiday_weekdays(exclusions=records)
    spans = contiguous_runs(
        days=[
            item.date
            for item in records
            if item.exclusion_type is ExclusionType.SCHOOL_HOLIDAY
        ],
    )
    blocks = (frozenset(span) & usable for span in spans)
    return [block for block in blocks if block]


def cancelled_services(
    *,
    exceptions: Iterable[CalendarException],
    on_dates: frozenset[date],
) -> frozenset[str]:
    """Find services withdrawn on every one of the given dates.

    Parameters
    ----------
    exceptions : Iterable[CalendarException]
        Single-day exceptions from the timetable.
    on_dates : frozenset[date]
        Dates a service must be absent from to qualify.

    Returns
    -------
    frozenset[str]
        Service identifiers cancelled on all of the dates. Empty when
        no dates are given, since every service would qualify.
    """
    if not on_dates:
        return frozenset()
    withdrawn: dict[str, set[date]] = {}
    for item in exceptions:
        if item.exception_type == SERVICE_REMOVED and item.date in on_dates:
            withdrawn.setdefault(item.service_id, set()).add(item.date)
    return frozenset(
        service for service, days in withdrawn.items() if days >= on_dates
    )


def covered_span(
    *,
    exceptions: Iterable[CalendarException],
) -> tuple[date, date] | None:
    """Find the range of dates a timetable bundle can speak about.

    Parameters
    ----------
    exceptions : Iterable[CalendarException]
        Single-day exceptions from the timetable.

    Returns
    -------
    tuple[date, date] | None
        Earliest and latest date present, or None when there are none.
    """
    days = [item.date for item in exceptions]
    if not days:
        return None
    return min(days), max(days)


def school_services(
    *,
    exceptions: Iterable[CalendarException],
    exclusions: Iterable[Exclusion],
) -> frozenset[str]:
    """Identify the school services a timetable bundle describes.

    Only breaks falling wholly inside the bundle's span are used. A
    bundle looks forward from the day it was published, so demanding
    absence from a break it does not reach would disqualify every
    service.

    Parameters
    ----------
    exceptions : Iterable[CalendarException]
        Single-day exceptions from the timetable.
    exclusions : Iterable[Exclusion]
        Exclusion records for the year.

    Returns
    -------
    frozenset[str]
        Service identifiers withdrawn across any whole school break
        the bundle covers. Empty when it covers no complete break.
    """
    items = list(exceptions)
    span = covered_span(exceptions=items)
    if span is None:
        return frozenset()
    found: set[str] = set()
    for block in holiday_blocks(exclusions=exclusions):
        if span[0] <= min(block) and max(block) <= span[1]:
            found |= cancelled_services(exceptions=items, on_dates=block)
    return frozenset(found)


def school_routes(
    *,
    trips: Iterable[TripRoute],
    services: frozenset[str],
) -> frozenset[str]:
    """Find routes run entirely by school services.

    A route with any trip on a surviving service is not school-only:
    it runs through the holidays, whatever its name suggests.

    Parameters
    ----------
    trips : Iterable[TripRoute]
        Scheduled trips with their route and service.
    services : frozenset[str]
        Service identifiers counted as school services.

    Returns
    -------
    frozenset[str]
        Routes whose every trip is on a school service.
    """
    by_route: dict[str, bool] = {}
    for trip in trips:
        school = trip.service_id in services
        by_route[trip.route_id] = by_route.get(trip.route_id, True) and school
    return frozenset(route for route, school in by_route.items() if school)
