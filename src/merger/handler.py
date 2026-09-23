"""Daily assembly of hourly partials into service-day facts.

Runs at 08:00 Sydney, not midnight. A trip can belong to one service
day while running past midnight, as late as hour 30, i.e. 06:00 the
next calendar day, and the window this reads runs to 07:00 to cover
it. Merging earlier finalises a service date before the hours it
claims to read have happened.

The window of partials to read is computed timezone-aware, since
Sydney observes daylight saving and the UTC offset changes mid-season.
"""

import os
from datetime import UTC, date, datetime, timedelta
from enum import StrEnum
from typing import Any, Final, TypedDict

import boto3
import duckdb
from aws_lambda_powertools import Logger
from aws_lambda_powertools.utilities.typing import LambdaContext

from common.collector_run import collector_run_query, source_day_globs
from common.connection import DuckDbLimits, configure
from common.curation import CurationRepository
from common.process import peak_rss_mb
from common.service_day import merge_window, service_date_for
from common.types_ import CurationJob, CurationRecord
from merger.merge_sql import (
    POSITION_MERGE,
    build_trip_query,
    build_trip_stop_query,
)
from merger.schedule import resolve_dim_source

logger = Logger()

DUCKDB_LIMITS: Final[DuckDbLimits] = DuckDbLimits(
    threads=2, memory_limit='2200MB',
)
"""Room for the merge.

The allocation buys about one vCPU, and the ceiling sits below the
function's own so DuckDB spills to disk before the runtime kills it.
"""

PARTIAL_PREFIX: Final[str] = 'curated/_partial'


class MergeTable(StrEnum):
    """One of the tables a merge run can assemble."""

    COLLECTOR_RUN = 'collector_run'
    TRIP = 'trip'
    TRIP_STOP = 'trip_stop'
    VEHICLE_POSITION = 'vehicle_position'


FROM_PARTIALS: Final[frozenset[MergeTable]] = frozenset({
    MergeTable.TRIP, MergeTable.TRIP_STOP, MergeTable.VEHICLE_POSITION,
})
"""Tables assembled from the hourly partials.

``collector_run`` is not one of them. It is folded from JSONL the
collector writes, which is kept far longer than a partial, so it can be
rebuilt for a day whose partials have already expired.
"""


def selected_tables(*, event: dict[str, Any]) -> frozenset[MergeTable]:
    """Choose which tables to assemble.

    Parameters
    ----------
    event : dict[str, Any]
        EventBridge event, optionally carrying a ``tables`` list.

    Returns
    -------
    frozenset[MergeTable]
        Every table when unset, matching the scheduled run.

    Raises
    ------
    ValueError
        If a requested name is not a table this merger writes. Named
        rather than ignored, so a typo in a hand-written payload does
        not quietly assemble nothing.
    """
    names = event.get('tables')
    if not names:
        return frozenset(MergeTable)
    try:
        return frozenset(MergeTable(name) for name in names)
    except ValueError as error:
        known = ', '.join(sorted(MergeTable))
        raise ValueError(
            f'unknown table in {names!r}; expected any of {known}',
        ) from error

PARTITION_MARGIN: Final[timedelta] = timedelta(days=1)
"""Slack on each side of the ``dt`` bounds a merge window implies."""


class UnmeasuredCurationCounts(TypedDict):
    """The ``CurationRecord`` fields the merger does not measure.

    The merger folds already-curated partials rather than raw feed
    objects, so these counters have no meaning here. Spreading this
    subset into a ``CurationRecord`` literal keeps mypy checking
    every key.
    """

    rows_in: int
    dupes_collapsed: int
    unjoined_route_ids: int
    dupes_differing_position: int
    unjoined_stop_ids: int
    unjoined_trip_ids: int


# Field order deliberately does not mirror CurationRecord's own
# declaration in types_.py: matching it line-for-line tripped
# pylint's duplicate-code check on what is otherwise coincidental
# overlap between two independent TypedDicts. ``objects_expected`` and
# ``objects_read`` are excluded: unlike these six, the merger *does*
# measure them, as partial-file coverage of the merge window (see
# ``count_partial_coverage``), so a missing hour is recorded rather
# than silently producing a short service day.
UNMEASURED_CURATION_COUNTS: Final[UnmeasuredCurationCounts] = {
    'rows_in': 0,
    'dupes_collapsed': 0,
    'unjoined_route_ids': 0,
    'dupes_differing_position': 0,
    'unjoined_stop_ids': 0,
    'unjoined_trip_ids': 0,
}


def target_service_date(
    *,
    event: dict[str, Any],
    now: datetime,
) -> date:
    """Choose which Sydney service date to assemble.

    Parameters
    ----------
    event : dict[str, Any]
        EventBridge event, optionally carrying a ``service_date``
        override for backfill.
    now : datetime
        Current UTC time.

    Returns
    -------
    date
        The service date to assemble.
    """
    override = event.get('service_date')
    if override:
        return date.fromisoformat(override)
    return service_date_for(instant=now) - timedelta(days=1)


def merge_collector_run(
    *,
    bucket: str,
    service_date: date,
    endpoint: str | None = None,
    session: boto3.Session | None = None,
) -> int:
    """Fold one Sydney day of collector JSONL into a Parquet table.

    Written to ``fact_collector_run``, a separate prefix. The source
    JSONL under ``collector_run`` is left in place: the collector
    keeps appending to it, and it is kept far longer than a partial,
    so a day can be rebuilt after its partials have expired.

    The source is partitioned by UTC fetch date and the output by
    Sydney calendar date, under ``collection_date`` rather than
    ``service_date``: the collector polls on the clock, so its day is
    midnight to midnight and not the timetable's day. One output day
    is cut from the two source partitions it spans. The cut is made
    with a named timezone rather than a fixed offset, because a
    Sydney day is 23 or 25 hours long across a daylight-saving
    transition.

    Parameters
    ----------
    bucket : str
        Bucket holding the curated layer.
    service_date : date
        Sydney date whose audit records to fold.
    endpoint : str | None
        Test-only S3 endpoint override, forwarded to ``configure``.
    session : boto3.Session | None
        Optional boto3 session, used to find which UTC days exist.
        Defaults to a new session.

    Returns
    -------
    int
        Rows written, or zero when neither source day exists.
    """
    client = (session or boto3.Session()).client('s3')
    globs = source_day_globs(
        client=client, bucket=bucket, service_date=service_date,
    )
    if not globs:
        logger.warning(
            'No collector audit records for this service day',
            extra={'service_date': f'{service_date:%Y-%m-%d}'},
        )
        return 0
    connection = duckdb.connect()
    configure(
        connection=connection, limits=DUCKDB_LIMITS, endpoint=endpoint,
    )
    target = (
        f's3://{bucket}/curated/fact_collector_run/'
        f'collection_date={service_date:%Y-%m-%d}/data.parquet'
    )
    connection.execute(
        f'COPY ({collector_run_query(globs=globs)}) '
        f"TO '{target}' (FORMAT PARQUET, COMPRESSION ZSTD)",
        {'day': f'{service_date:%Y-%m-%d}'},
    )
    return count_parquet_rows(connection=connection, target=target)


def partial_paths(
    *,
    client: Any,
    bucket: str,
    table: str,
    bounds: tuple[str, str],
) -> list[str]:
    """List one table's partial objects inside the ``dt`` bounds.

    DuckDB is handed these paths rather than a glob. Reading partials
    by column name makes it open every file a glob matches, so a glob
    would make each merge read every partial still alive, and fail on
    any unreadable one, however far outside the bounds.

    Parameters
    ----------
    client : Any
        A boto3 S3 client.
    bucket : str
        Bucket holding the curated layer.
    table : str
        One of ``vehicle_position``, ``trip_stop`` or ``trip``.
    bounds : tuple[str, str]
        Inclusive ``dt`` partition values, from ``partition_bounds``.

    Returns
    -------
    list[str]
        S3 paths, empty when no partial lies inside the bounds.
    """
    first, last = (date.fromisoformat(bound) for bound in bounds)
    days = (
        first + timedelta(days=offset)
        for offset in range((last - first).days + 1)
    )
    pages = (
        page
        for day in days
        for page in client.get_paginator('list_objects_v2').paginate(
            Bucket=bucket, Prefix=f'{PARTIAL_PREFIX}/{table}/dt={day}/',
        )
    )
    return [
        f's3://{bucket}/{item["Key"]}'
        for page in pages
        for item in page.get('Contents', ())
    ]


def copy_merge(
    *,
    connection: duckdb.DuckDBPyConnection,
    query: str,
    target: str,
    parameters: dict[str, Any],
) -> int:
    """Write one merge query's result to a Parquet object.

    Parameters
    ----------
    connection : duckdb.DuckDBPyConnection
        Configured DuckDB connection.
    query : str
        The merge query.
    target : str
        S3 path to write.
    parameters : dict[str, Any]
        Query parameters. ``partials`` is the list of paths to read.

    Returns
    -------
    int
        Rows written, or zero without writing anything when there are
        no partials to read, since DuckDB refuses an empty file list.
    """
    if not parameters['partials']:
        logger.warning('No partials to merge', extra={'target': target})
        return 0
    connection.execute(
        f"COPY ({query}) TO '{target}' (FORMAT PARQUET, COMPRESSION ZSTD)",
        parameters,
    )
    return count_parquet_rows(connection=connection, target=target)


def expected_hours(
    *,
    window: tuple[datetime, datetime],
) -> list[datetime]:
    """List the UTC hour boundaries a merge window implies a partial for.

    Parameters
    ----------
    window : tuple[datetime, datetime]
        UTC start and end of the hours to read, as returned by
        ``merge_window``.

    Returns
    -------
    list[datetime]
        One entry per hour the compactor should have run for, floored
        to the hour, matching how the compactor names its partials.
    """
    start, end = window
    cursor = start.replace(minute=0, second=0, microsecond=0)
    hours: list[datetime] = []
    while cursor < end:
        hours.append(cursor)
        cursor += timedelta(hours=1)
    return hours


def partition_bounds(
    *,
    window: tuple[datetime, datetime],
) -> tuple[str, str]:
    """Bound the ``dt`` partitions a merge window can touch.

    The bounds prune whole partial objects by their partition path,
    before any Parquet footer is read, so a merge costs what the window
    holds rather than everything ever collected. They are deliberately
    loose: a trip is listed in the feed for hours before it departs, so
    a service date can first appear in a partial written before its own
    window opens.

    Parameters
    ----------
    window : tuple[datetime, datetime]
        UTC start and end of the hours to read, as returned by
        ``merge_window``.

    Returns
    -------
    tuple[str, str]
        Inclusive ``dt`` partition values, as ``YYYY-MM-DD``.
    """
    start, end = window
    return (
        f'{(start - PARTITION_MARGIN).date():%Y-%m-%d}',
        f'{(end + PARTITION_MARGIN).date():%Y-%m-%d}',
    )


def partial_hour_from_key(*, key: str) -> datetime:
    """Recover the partitioned hour encoded in a partial object's key.

    The inverse of ``partial_key`` in ``compactor.handler``, which
    builds ``curated/_partial/<table>/dt=YYYY-MM-DD/hour=HH/
    data.parquet``. Do not use ``fetched_at_from_key`` in
    ``common.raw_read`` for these keys. It parses a different
    shape, with an ``HHMMSS`` filename rather than an ``hour=HH``
    segment.

    Parameters
    ----------
    key : str
        Full S3 key of one partial object.

    Returns
    -------
    datetime
        Timezone-aware UTC instant, floored to the hour.

    Raises
    ------
    ValueError
        If ``key`` does not have the expected shape.
    """
    try:
        parts = key.split('/')
        day = parts[3].removeprefix('dt=')
        hour = parts[4].removeprefix('hour=')
        return datetime.strptime(
            f'{day} {hour}', '%Y-%m-%d %H',
        ).replace(tzinfo=UTC)
    except (IndexError, ValueError) as error:
        raise ValueError(
            f'malformed partial object key: {key!r}',
        ) from error


def count_existing_partials(
    *,
    client: Any,
    bucket: str,
    table: str,
    expected: set[datetime],
) -> int:
    """Count one table's partials that fall inside a merge window.

    Parameters
    ----------
    client : Any
        A boto3 S3 client.
    bucket : str
        Bucket holding the curated layer.
    table : str
        One of ``vehicle_position``, ``trip_stop`` or ``trip``.
    expected : set[datetime]
        Hours the merge window covers, from ``expected_hours``.

    Returns
    -------
    int
        Number of partials present for an expected hour.
    """
    pages = client.get_paginator('list_objects_v2').paginate(
        Bucket=bucket, Prefix=f'{PARTIAL_PREFIX}/{table}/',
    )
    keys = (
        item['Key']
        for page in pages
        for item in page.get('Contents', ())
    )
    return sum(
        1 for key in keys if partial_hour_from_key(key=key) in expected
    )


def count_partial_coverage(
    *,
    bucket: str,
    window: tuple[datetime, datetime],
    session: boto3.Session | None = None,
) -> tuple[int, int]:
    """Compare the partials a merge window implies against what exists.

    An hour can go missing through a compactor failure, or because a
    partial expired before the merger ran. Either way it shows up as
    a short day in ``curation_run`` rather than reading as a quiet
    night.

    Parameters
    ----------
    bucket : str
        Bucket holding the curated layer.
    window : tuple[datetime, datetime]
        UTC range of partials to read.
    session : boto3.Session | None
        Optional boto3 session. Defaults to a new session.

    Returns
    -------
    tuple[int, int]
        ``objects_expected`` (hours in the window, times the tables
        built from partials) and ``objects_read`` (how many of those
        actually exist).
    """
    hours = expected_hours(window=window)
    expected = set(hours)
    client = (session or boto3.Session()).client('s3')
    read = sum(
        count_existing_partials(
            client=client, bucket=bucket, table=table, expected=expected,
        )
        for table in FROM_PARTIALS
    )
    return len(hours) * len(FROM_PARTIALS), read


def merge_trip_stops(
    *,
    connection: duckdb.DuckDBPyConnection,
    bucket: str,
    service_date: date,
    session: boto3.Session | None = None,
) -> int:
    """Fold one service day's trip-stop partials into one table.

    Parameters
    ----------
    connection : duckdb.DuckDBPyConnection
        Configured DuckDB connection.
    bucket : str
        Bucket holding the curated layer.
    service_date : date
        The Sydney service date being assembled.
    session : boto3.Session | None
        Optional boto3 session, used to list dimension snapshots.
        Defaults to a new session.

    Returns
    -------
    int
        Rows written.
    """
    return merge_trip_table(
        connection=connection, bucket=bucket, service_date=service_date,
        session=session or boto3.Session(),
        table=MergeTable.TRIP_STOP,
    )


def merge_trips(
    *,
    connection: duckdb.DuckDBPyConnection,
    bucket: str,
    service_date: date,
    session: boto3.Session | None = None,
) -> int:
    """Fold one service day's trip-status partials into one table.

    Parameters
    ----------
    connection : duckdb.DuckDBPyConnection
        Configured DuckDB connection.
    bucket : str
        Bucket holding the curated layer.
    service_date : date
        The Sydney service date being assembled.
    session : boto3.Session | None
        Optional boto3 session, used to list dimension snapshots.
        Defaults to a new session.

    Returns
    -------
    int
        Rows written.
    """
    return merge_trip_table(
        connection=connection, bucket=bucket, service_date=service_date,
        session=session or boto3.Session(),
        table=MergeTable.TRIP,
    )


def merge_trip_table(
    *,
    connection: duckdb.DuckDBPyConnection,
    bucket: str,
    service_date: date,
    session: boto3.Session,
    table: MergeTable,
) -> int:
    """Fold one service day of a timetable-dated table.

    Both trip tables file a trip under the service day its timetable
    gives, so both read the snapshot in effect.

    Parameters
    ----------
    connection : duckdb.DuckDBPyConnection
        Configured DuckDB connection.
    bucket : str
        Bucket holding the curated layer.
    service_date : date
        The Sydney service date being assembled. Its merge window
        bounds the partitions scanned.
    session : boto3.Session
        Session used to list partials and dimension snapshots.
    table : MergeTable
        Either ``trip_stop`` or ``trip``.

    Returns
    -------
    int
        Rows written.
    """
    dim_source = resolve_dim_source(
        bucket=bucket, service_date=service_date, session=session,
    )
    build = (
        build_trip_stop_query if table is MergeTable.TRIP_STOP
        else build_trip_query
    )
    bounds = partition_bounds(window=merge_window(service_date=service_date))
    return copy_merge(
        connection=connection,
        query=build(dim_source=dim_source),
        target=(
            f's3://{bucket}/curated/fact_{table}/'
            f'service_date={service_date:%Y-%m-%d}/data.parquet'
        ),
        parameters={
            'partials': partial_paths(
                client=session.client('s3'), bucket=bucket, table=table,
                bounds=bounds,
            ),
            'service_date': f'{service_date:%Y%m%d}',
            'dt_from': bounds[0],
            'dt_to': bounds[1],
        },
    )


def merge_positions(
    *,
    connection: duckdb.DuckDBPyConnection,
    bucket: str,
    service_date: date,
    window: tuple[datetime, datetime],
    session: boto3.Session | None = None,
) -> int:
    """Fold one service day's position partials into one table.

    Parameters
    ----------
    connection : duckdb.DuckDBPyConnection
        Configured DuckDB connection.
    bucket : str
        Bucket holding the curated layer.
    service_date : date
        The Sydney service date being assembled.
    window : tuple[datetime, datetime]
        UTC range of partials to read.
    session : boto3.Session | None
        Optional boto3 session, used to list partials. Defaults to a
        new session.

    Returns
    -------
    int
        Rows written.
    """
    dt_from, dt_to = partition_bounds(window=window)
    return copy_merge(
        connection=connection,
        query=POSITION_MERGE,
        target=(
            f's3://{bucket}/curated/fact_vehicle_position/'
            f'service_date={service_date:%Y-%m-%d}/data.parquet'
        ),
        parameters={
            'partials': partial_paths(
                client=(session or boto3.Session()).client('s3'),
                bucket=bucket, table=MergeTable.VEHICLE_POSITION,
                bounds=(dt_from, dt_to),
            ),
            'window_start': window[0],
            'window_end': window[1],
            'dt_from': dt_from,
            'dt_to': dt_to,
        },
    )


def merge_partial_tables(
    *,
    connection: duckdb.DuckDBPyConnection,
    bucket: str,
    service_date: date,
    tables: frozenset[MergeTable],
    session: boto3.Session,
) -> tuple[int, ...]:
    """Assemble every requested table that is built from partials.

    Parameters
    ----------
    connection : duckdb.DuckDBPyConnection
        Configured DuckDB connection.
    bucket : str
        Bucket holding the curated layer.
    service_date : date
        The Sydney service date being assembled.
    tables : frozenset[MergeTable]
        Tables requested for this run.
    session : boto3.Session
        Session used to list dimension snapshots.

    Returns
    -------
    tuple[int, ...]
        Rows written to ``fact_trip_stop``, ``fact_vehicle_position``
        and ``fact_trip``, zero for a table not requested.
    """
    window = merge_window(service_date=service_date)
    stops = MergeTable.TRIP_STOP in tables and merge_trip_stops(
        connection=connection, bucket=bucket, service_date=service_date,
        session=session,
    )
    positions = MergeTable.VEHICLE_POSITION in tables and merge_positions(
        connection=connection, bucket=bucket, service_date=service_date,
        window=window, session=session,
    )
    trips = MergeTable.TRIP in tables and merge_trips(
        connection=connection, bucket=bucket, service_date=service_date,
        session=session,
    )
    return int(stops), int(positions), int(trips)


def count_parquet_rows(
    *,
    connection: duckdb.DuckDBPyConnection,
    target: str,
) -> int:
    """Count rows in a Parquet object just written.

    Parameters
    ----------
    connection : duckdb.DuckDBPyConnection
        Open connection.
    target : str
        Path to the Parquet object.

    Returns
    -------
    int
        Row count.
    """
    result = connection.execute(
        f"SELECT COUNT(*) FROM read_parquet('{target}')",
    ).fetchone()
    return int(result[0]) if result else 0


def build_record(
    *,
    context: LambdaContext,
    started: datetime,
    service_date: date,
    rows_written: tuple[int, ...],
    partial_coverage: tuple[int, int],
) -> CurationRecord:
    """Assemble one invocation's audit record.

    Parameters
    ----------
    context : LambdaContext
        Lambda context, used for the invocation id.
    started : datetime
        UTC instant the invocation began.
    service_date : date
        The Sydney service date assembled.
    rows_written : tuple[int, ...]
        Rows written to each table assembled.
    partial_coverage : tuple[int, int]
        ``objects_expected`` and ``objects_read``, from
        ``count_partial_coverage``.

    Returns
    -------
    CurationRecord
        The audit record for this run.
    """
    objects_expected, objects_read = partial_coverage
    return {
        **UNMEASURED_CURATION_COUNTS,
        'job': CurationJob.MERGER.value,
        'invocation_id': context.aws_request_id,
        'started_at_utc': started.isoformat(),
        'finished_at_utc': datetime.now(tz=UTC).isoformat(),
        'partition': f'{service_date:%Y-%m-%d}',
        'objects_expected': objects_expected,
        'objects_read': objects_read,
        'rows_out': sum(rows_written),
        'peak_rss_mb': peak_rss_mb(),
        'error': None,
    }


def require_partials(
    *,
    coverage: tuple[int, int],
    service_date: date,
) -> None:
    """Refuse to assemble a day whose partials have all gone.

    A merge reads the partials through a glob spanning every date, so an
    expired window matches no files rather than failing, and the day
    would be rewritten as an empty table over a good one. Partials are
    kept for days and facts forever, so a re-run long after the fact is
    the expected way to hit this.

    Parameters
    ----------
    coverage : tuple[int, int]
        ``objects_expected`` and ``objects_read``, from
        ``count_partial_coverage``.
    service_date : date
        The Sydney service date being assembled.

    Raises
    ------
    RuntimeError
        If no partial covering the window still exists.
    """
    if not coverage[1]:
        raise RuntimeError(
            f'no partials remain for service day {service_date:%Y-%m-%d}',
        )


def warn_on_short_day(*, coverage: tuple[int, int]) -> None:
    """Log a warning when fewer partials exist than the window implies.

    Never raises. A missing hour produces a short day, not a failure,
    and the merge can be re-run later once the partial exists.

    Parameters
    ----------
    coverage : tuple[int, int]
        ``objects_expected`` and ``objects_read``, from
        ``count_partial_coverage``.
    """
    objects_expected, objects_read = coverage
    if objects_read < objects_expected:
        logger.warning(
            'Merger assembling a short day',
            extra={
                'objects_expected': objects_expected,
                'objects_read': objects_read,
            },
        )


@logger.inject_lambda_context
def handler(
    event: dict[str, Any],
    context: LambdaContext,
    *,
    endpoint: str | None = None,
) -> CurationRecord:
    """Assemble one Sydney service day from hourly partials.

    Parameters
    ----------
    event : dict[str, Any]
        EventBridge event, optionally carrying a ``service_date``
        override and a ``tables`` list naming which tables to
        assemble. Both are for re-runs; the scheduled event carries
        neither and assembles every table for yesterday.
    context : LambdaContext
        Lambda context, used for the invocation id.
    endpoint : str | None
        Test-only S3 endpoint override, forwarded to ``configure`` and
        ``merge_collector_run``. Lambda invokes the handler with two
        positional arguments, so this is always None in production.

    Returns
    -------
    CurationRecord
        The audit record written for this run.

    Raises
    ------
    RuntimeError
        If a table built from partials was asked for and none remain.
    """
    bucket = os.environ['BUCKET_NAME']
    started = datetime.now(tz=UTC)
    service_date = target_service_date(event=event, now=started)
    tables = selected_tables(event=event)
    window = merge_window(service_date=service_date)
    session = boto3.Session()
    coverage = count_partial_coverage(
        bucket=bucket, window=window, session=session,
    )
    if tables & FROM_PARTIALS:
        require_partials(coverage=coverage, service_date=service_date)
        warn_on_short_day(coverage=coverage)
    connection = duckdb.connect()
    configure(
        connection=connection, limits=DUCKDB_LIMITS, endpoint=endpoint,
    )
    collector_rows = 0
    if MergeTable.COLLECTOR_RUN in tables:
        collector_rows = merge_collector_run(
            bucket=bucket, service_date=service_date, endpoint=endpoint,
            session=session,
        )
    partial_rows = merge_partial_tables(
        connection=connection, bucket=bucket, service_date=service_date,
        tables=tables, session=session,
    )
    record = build_record(
        context=context, started=started, service_date=service_date,
        rows_written=(collector_rows, *partial_rows),
        partial_coverage=coverage,
    )
    CurationRepository(bucket=bucket, session=session).put_record(
        record=record,
    )
    return record
