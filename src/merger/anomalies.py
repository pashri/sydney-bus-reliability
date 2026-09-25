"""Checks run on each service day the merger has just written.

Each check is an invariant a correct day always meets. A breach means
the feed did something new or the pipeline mishandled it, so the day
should be looked at while its partials still exist and a re-merge is
cheap; after that only a replay from raw repairs it.

Every run emits one ``Anomalies`` metric, the number of checks
breached, zero included, and logs every measure. An alarm on that
metric raises the alert.
"""

from dataclasses import asdict, dataclass
from typing import Any, Final

import duckdb
from aws_lambda_powertools import Logger, Metrics
from aws_lambda_powertools.metrics import MetricUnit

logger = Logger()
metrics = Metrics(namespace='SydneyBusReliability', service='merger')

FAR_FROM_SCHEDULE_S: Final[int] = 12 * 3600  # seconds
EARLIEST_PLAUSIBLE: Final[str] = '2000-01-01 00:00:00+00'
MAX_UNSCHEDULED_SHARE: Final[float] = 0.03
"""Share of stop rows with no scheduled time. Days that borrow a later
timetable run higher than ordinary days, and stay well under this."""
MIN_RELIABLE_SHARE: Final[float] = 0.80
"""Share of SCHEDULED stop rows that are reliable. Ordinary days sit a
few points above it."""
MIN_TRIPS: Final[int] = 15_000
"""Trips in ``fact_trip``. A weekend day has well over this."""
ZERO_CHECKS: Final[tuple[str, ...]] = (
    'times_far_from_schedule',
    'times_before_2000',
    'duplicate_stop_keys',
    'duplicate_trip_keys',
    'inconsistent_trips',
)

STOP_SQL: Final[str] = f"""
select
    count(*) filter (
        abs(epoch(final_predicted_arrival_utc)
            - epoch(scheduled_arrival_utc)) > {FAR_FROM_SCHEDULE_S}
        and final_predicted_arrival_utc >= TIMESTAMPTZ '{EARLIEST_PLAUSIBLE}'
        or abs(epoch(final_predicted_departure_utc)
            - epoch(scheduled_arrival_utc)) > {FAR_FROM_SCHEDULE_S}
        and final_predicted_departure_utc
            >= TIMESTAMPTZ '{EARLIEST_PLAUSIBLE}'
    ),
    count(*) filter (
        final_predicted_arrival_utc < TIMESTAMPTZ '{EARLIEST_PLAUSIBLE}'
        or final_predicted_departure_utc
            < TIMESTAMPTZ '{EARLIEST_PLAUSIBLE}'
    ),
    count(*) filter (trip_id <> '' and scheduled_arrival_utc is null)
        / nullif(count(*) filter (trip_id <> ''), 0),
    count(*) filter (is_reliable)
        / nullif(count(*) filter (schedule_relationship = 'SCHEDULED'), 0)
from read_parquet(?)
"""
"""Times off the schedule by half a day or more, times before 2000,
and the two shares. A time before 2000 counts once, as that."""

DUPLICATE_STOP_SQL: Final[str] = """
select count(*) from (
    select trip_id, stop_id, stop_sequence
    from read_parquet(?)
    where trip_id <> ''
    group by all
    having count(*) > 1
)
"""
"""Rows with no trip id are the feed's unkeyed runs; they share keys."""

TRIP_SQL: Final[str] = """
select
    count(*),
    count(*) - count(distinct trip_id),
    count(*) filter (
        final_status = 'CANCELED' and canceled_polls = 0
        or canceled_polls > 0 and first_canceled_at_utc is null
        or first_canceled_at_utc > last_canceled_at_utc
        or first_seen_at_utc > last_seen_at_utc
        or service_date not in (
            strptime(start_date, '%Y%m%d')::date,
            strptime(start_date, '%Y%m%d')::date - 1
        )
    )
from read_parquet(?)
"""
"""Trips, duplicated trips, and rows contradicting themselves. The
service day is the feed's start date, or the day before it for a trip
timetabled after midnight."""


@dataclass(frozen=True)
class DayMeasures:  # pylint: disable=too-many-instance-attributes
    """What the checks measured on one service day.

    Attributes
    ----------
    times_far_from_schedule : int
        Stop rows with a time half a day or more from its schedule.
    times_before_2000 : int
        Stop rows with a time before 2000, an epoch read literally.
    duplicate_stop_keys : int
        Stop keys held by more than one row.
    duplicate_trip_keys : int
        Surplus rows for trips listed more than once.
    inconsistent_trips : int
        Trip rows contradicting themselves.
    unscheduled_share : float
        Share of stop rows with no scheduled time.
    reliable_share : float
        Share of SCHEDULED stop rows that are reliable.
    trips : int
        Rows in ``fact_trip``.
    """

    times_far_from_schedule: int
    times_before_2000: int
    duplicate_stop_keys: int
    duplicate_trip_keys: int
    inconsistent_trips: int
    unscheduled_share: float
    reliable_share: float
    trips: int


@dataclass(frozen=True)
class Breach:
    """One check a day failed.

    Attributes
    ----------
    name : str
        The measure that failed.
    value : float
        What it measured.
    limit : float
        The bound it crossed.
    """

    name: str
    value: float
    limit: float


def fact_path(*, bucket: str, table: str, service_date: str) -> str:
    """Name a merged fact file.

    Parameters
    ----------
    bucket : str
        Bucket holding the curated layer.
    table : str
        Fact table without its prefix, e.g. ``trip_stop``.
    service_date : str
        ``YYYY-MM-DD``.

    Returns
    -------
    str
        The file's S3 URL.
    """
    return (
        f's3://{bucket}/curated/fact_{table}/'
        f'service_date={service_date}/data.parquet'
    )


def measure_day(
    *,
    connection: duckdb.DuckDBPyConnection,
    trip_stop_path: str,
    trip_path: str,
) -> DayMeasures:
    """Measure one day's facts.

    Only numbers are fetched, never timestamps, so no timezone library
    is needed to read the results.

    Parameters
    ----------
    connection : duckdb.DuckDBPyConnection
        Connection able to read both files, with ICU loaded.
    trip_stop_path : str
        The day's ``fact_trip_stop`` file.
    trip_path : str
        The day's ``fact_trip`` file.

    Returns
    -------
    DayMeasures
        Every measure.
    """
    far, early, unscheduled, reliable = fetch(
        connection=connection, sql=STOP_SQL, path=trip_stop_path,
    )
    (duplicate_stops,) = fetch(
        connection=connection, sql=DUPLICATE_STOP_SQL, path=trip_stop_path,
    )
    trips, duplicate_trips, inconsistent = fetch(
        connection=connection, sql=TRIP_SQL, path=trip_path,
    )
    return DayMeasures(
        times_far_from_schedule=far,
        times_before_2000=early,
        duplicate_stop_keys=duplicate_stops,
        duplicate_trip_keys=duplicate_trips,
        inconsistent_trips=inconsistent,
        unscheduled_share=unscheduled or 0.0,
        reliable_share=reliable or 0.0,
        trips=trips,
    )


def fetch(
    *,
    connection: duckdb.DuckDBPyConnection,
    sql: str,
    path: str,
) -> tuple[Any, ...]:
    """Run one single-row query over one file.

    Parameters
    ----------
    connection : duckdb.DuckDBPyConnection
        Connection to run it on.
    sql : str
        Query with one parameter, the file.
    path : str
        File to read.

    Returns
    -------
    tuple[Any, ...]
        The row.

    Raises
    ------
    RuntimeError
        If the query returned no row.
    """
    row = connection.execute(sql, [path]).fetchone()
    if row is None:
        raise RuntimeError(f'no result reading {path}')
    return tuple(row)


def breaches(*, measures: DayMeasures) -> list[Breach]:
    """Compare a day's measures with their bounds.

    Parameters
    ----------
    measures : DayMeasures
        One day's measures.

    Returns
    -------
    list[Breach]
        Every check failed, in field order.
    """
    values = asdict(measures)
    found = [
        Breach(name=name, value=values[name], limit=0)
        for name in ZERO_CHECKS if values[name] > 0
    ]
    if measures.unscheduled_share > MAX_UNSCHEDULED_SHARE:
        found.append(Breach(
            name='unscheduled_share', value=measures.unscheduled_share,
            limit=MAX_UNSCHEDULED_SHARE,
        ))
    if measures.reliable_share < MIN_RELIABLE_SHARE:
        found.append(Breach(
            name='reliable_share', value=measures.reliable_share,
            limit=MIN_RELIABLE_SHARE,
        ))
    if measures.trips < MIN_TRIPS:
        found.append(Breach(
            name='trips', value=measures.trips, limit=MIN_TRIPS,
        ))
    return found


def report(*, measures: DayMeasures, service_date: str) -> int:
    """Log a day's measures and emit its breach count.

    Parameters
    ----------
    measures : DayMeasures
        One day's measures.
    service_date : str
        ``YYYY-MM-DD``.

    Returns
    -------
    int
        Checks breached.
    """
    found = breaches(measures=measures)
    logger.info(
        'Day checked',
        extra={'service_date': service_date, **asdict(measures)},
    )
    for breach in found:
        logger.warning('Anomaly', extra={
            'service_date': service_date, 'check': breach.name,
            'value': breach.value, 'limit': breach.limit,
        })
    metrics.add_metric(
        name='Anomalies', unit=MetricUnit.Count, value=len(found),
    )
    metrics.flush_metrics()
    return len(found)


def check_day(
    *,
    connection: duckdb.DuckDBPyConnection,
    bucket: str,
    service_date: str,
) -> int:
    """Check the facts just merged for one day and report.

    Parameters
    ----------
    connection : duckdb.DuckDBPyConnection
        Connection able to read the bucket, with ICU loaded.
    bucket : str
        Bucket holding the curated layer.
    service_date : str
        ``YYYY-MM-DD``.

    Returns
    -------
    int
        Checks breached.
    """
    measures = measure_day(
        connection=connection,
        trip_stop_path=fact_path(
            bucket=bucket, table='trip_stop', service_date=service_date,
        ),
        trip_path=fact_path(
            bucket=bucket, table='trip', service_date=service_date,
        ),
    )
    return report(measures=measures, service_date=service_date)
