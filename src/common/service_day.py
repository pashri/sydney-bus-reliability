"""Sydney service-day arithmetic.

A GTFS trip can belong to one service day while running past midnight.
Its hour field climbs past 24 to say so, e.g. ``25:15:30`` means 01:15
the next calendar day. Hour 30 appears in the real bundle. Sydney also
observes daylight saving, so its UTC offset changes mid-season.
"""

from datetime import UTC, date, datetime, timedelta
from typing import Final
from zoneinfo import ZoneInfo

SYDNEY: Final[ZoneInfo] = ZoneInfo('Australia/Sydney')
LEAD_MARGIN: Final[timedelta] = timedelta(hours=1)
"""Slack before the Sydney day starts, covering late-written objects."""

TRAIL_MARGIN: Final[timedelta] = timedelta(hours=7)
"""Slack after the Sydney day ends.

Six hours covers the maximum GTFS hour of 30, plus one hour of the same
slack applied at the leading edge.
"""


def parse_gtfs_time(*, value: str) -> timedelta:
    """Parse a GTFS ``HH:MM:SS`` offset, allowing hours past 24.

    Parameters
    ----------
    value : str
        Time as written in ``stop_times.txt``, e.g. ``25:15:30``.

    Returns
    -------
    timedelta
        Offset from midnight of the service date.

    Raises
    ------
    ValueError
        If the value is not three colon-separated integers.
    """
    hours, minutes, seconds = (int(part) for part in value.split(':'))
    return timedelta(hours=hours, minutes=minutes, seconds=seconds)


def scheduled_instant(*, start_date: str, gtfs_time: str) -> datetime:
    """Resolve a GTFS time on a service date to a UTC instant.

    Parameters
    ----------
    start_date : str
        Service date as ``YYYYMMDD``.
    gtfs_time : str
        Time as written in ``stop_times.txt``.

    Returns
    -------
    datetime
        Timezone-aware UTC instant.

    Notes
    -----
    The GTFS offset is added as wall-clock time on top of local
    midnight. The spec's literal wording is "noon minus 12 hours",
    i.e. elapsed seconds from a fixed anchor. The two agree except
    across a daylight-saving transition, where only the wall-clock
    reading reproduces the printed timetable.

    Which convention TfNSW uses is not confirmed.
    """
    midnight = datetime.strptime(start_date, '%Y%m%d').replace(
        tzinfo=SYDNEY,
    )
    return (midnight + parse_gtfs_time(value=gtfs_time)).astimezone(UTC)


def service_date_for(*, instant: datetime) -> date:
    """Map a UTC instant to the Sydney calendar date containing it.

    This is the calendar date, not the GTFS service date - a trip that
    runs past midnight keeps the service date carried on its own
    ``start_date`` field, which is authoritative and used in preference.

    Parameters
    ----------
    instant : datetime
        Timezone-aware UTC instant.

    Returns
    -------
    date
        Sydney-local date.
    """
    return instant.astimezone(SYDNEY).date()


def merge_window(*, service_date: date) -> tuple[datetime, datetime]:
    """Compute the UTC range of partials covering one service day.

    Parameters
    ----------
    service_date : date
        The Sydney service date being assembled.

    Returns
    -------
    tuple[datetime, datetime]
        Inclusive UTC start and end of the hours to read.
    """
    midnight = datetime(
        service_date.year,
        service_date.month,
        service_date.day,
        tzinfo=SYDNEY,
    )
    start = (midnight - LEAD_MARGIN).astimezone(UTC)
    end = (midnight + timedelta(days=1) + TRAIL_MARGIN).astimezone(UTC)
    return start, end
