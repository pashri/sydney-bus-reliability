"""Daily assembly of hourly partials into service-day facts.

Runs at ~04:00 Sydney rather than midnight. GTFS lets a trip belong to
the previous service day while running past midnight - a measured
maximum hour of 30, i.e. 06:00 the next calendar day - so compacting at
midnight would finalise a service date before its last trips had
finished reporting.

Reads partials across a window computed timezone-aware, because Sydney
moves from UTC+10 to UTC+11 on 4 October 2026, inside the collection
window.
"""

import os
from datetime import UTC, date, datetime, timedelta
from typing import Any, Final, TypedDict

import boto3
import duckdb
import pyarrow as pa
from aws_lambda_powertools import Logger
from aws_lambda_powertools.utilities.typing import LambdaContext

from src.common.curation import CurationRepository
from src.common.process import peak_rss_mb
from src.common.service_day import (
    merge_window,
    scheduled_instant,
    service_date_for,
)
from src.common.types_ import CurationJob, CurationRecord
from src.merger.merge_sql import POSITION_MERGE, TRIP_STOP_MERGE

logger = Logger()

PARTIAL_PREFIX: Final[str] = 'curated/_partial'


class UnmeasuredCurationCounts(TypedDict):
    """The ``CurationRecord`` fields the merger does not measure.

    Unlike the compactor, the merger folds already-curated partials
    rather than raw feed objects, so these counters have no meaning
    here. A ``TypedDict`` subset lets it be spread into the full
    ``CurationRecord`` literal with mypy still checking every key.
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


def configure(
    *,
    connection: duckdb.DuckDBPyConnection,
    endpoint: str | None = None,
) -> None:
    """Prepare a DuckDB connection for S3 access.

    Parameters
    ----------
    connection : duckdb.DuckDBPyConnection
        Connection to configure.
    endpoint : str | None
        Override S3 endpoint, host[:port] only. A test seam: it is
        never set in production, where DuckDB talks to real AWS over
        TLS, and is passed explicitly by tests that run a local moto
        server, since DuckDB's httpfs makes its own socket
        connections and so cannot be redirected by ``mock_aws()``.
    """
    connection.execute('INSTALL httpfs; LOAD httpfs;')
    # CHAIN 'env' pins credential resolution to environment variables,
    # which is what both the Lambda runtime and the test fixtures set,
    # rather than DuckDB's default order which checks a local
    # ~/.aws/credentials profile first and can pick up stale keys.
    options = "PROVIDER credential_chain, CHAIN 'env'"
    if endpoint:
        options += (
            f", ENDPOINT '{endpoint}', URL_STYLE 'path', USE_SSL false"
        )
    connection.execute(f'CREATE SECRET (TYPE s3, {options});')


def merge_collector_run(
    *,
    bucket: str,
    service_date: date,
    endpoint: str | None = None,
) -> int:
    """Fold one day of collector JSONL into a Parquet table.

    Written to ``fact_collector_run``, a separate prefix, because the
    collector keeps writing JSONL to ``collector_run`` and
    ``check_collection.py`` still reads it. The JSONL gets no lifecycle
    rule: expiring it would silently shorten the health tool's history.

    Parameters
    ----------
    bucket : str
        Bucket holding the curated layer.
    service_date : date
        Sydney date whose audit records to fold.
    endpoint : str | None
        Test-only S3 endpoint override, forwarded to ``configure``.

    Returns
    -------
    int
        Rows written.
    """
    connection = duckdb.connect()
    configure(connection=connection, endpoint=endpoint)
    source = (
        f's3://{bucket}/curated/collector_run/'
        f'dt={service_date:%Y-%m-%d}/*.jsonl'
    )
    target = (
        f's3://{bucket}/curated/fact_collector_run/'
        f'service_date={service_date:%Y-%m-%d}/data.parquet'
    )
    connection.execute(
        f"COPY (SELECT * FROM read_json_auto('{source}')) "
        f"TO '{target}' (FORMAT PARQUET, COMPRESSION SNAPPY)",
    )
    return count_rows(connection=connection, source=source)


def count_rows(
    *,
    connection: duckdb.DuckDBPyConnection,
    source: str,
) -> int:
    """Count rows in a JSON source.

    Parameters
    ----------
    connection : duckdb.DuckDBPyConnection
        Open connection.
    source : str
        Path or glob to read.

    Returns
    -------
    int
        Row count.
    """
    result = connection.execute(
        f"SELECT COUNT(*) FROM read_json_auto('{source}')",
    ).fetchone()
    return int(result[0]) if result else 0


def partial_glob(*, bucket: str, table: str) -> str:
    """Build the S3 glob of every hourly partial for one table.

    One glob across all UTC dates, rather than one per date the merge
    window touches: DuckDB's ``read_parquet`` errors on a glob list
    entry that matches zero files, which a quiet compactor gap would
    trigger for no good reason. Narrowing to the service day is left
    to each merge SQL's own ``WHERE`` clause instead.

    Parameters
    ----------
    bucket : str
        Bucket holding the curated layer.
    table : str
        Either ``vehicle_position`` or ``trip_stop``.

    Returns
    -------
    str
        A glob matching every partial ever written for this table.
    """
    return (
        f's3://{bucket}/{PARTIAL_PREFIX}/{table}/dt=*/hour=*/data.parquet'
    )


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


def partial_hour_from_key(*, key: str) -> datetime:
    """Recover the partitioned hour encoded in a partial object's key.

    The inverse of ``partial_key`` in ``src.compactor.handler``, which
    builds ``curated/_partial/<table>/dt=YYYY-MM-DD/hour=HH/
    data.parquet``. Not ``fetched_at_from_key`` in
    ``src.common.raw_read``: that parses a different key shape, with a
    ``HHMMSS`` filename rather than an ``hour=HH`` segment.

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
        Either ``vehicle_position`` or ``trip_stop``.
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

    A missing hour - a compactor failure, or a partial aged out by the
    3-day ``ExpirePartials`` lifecycle before the merger ran - must
    show up as a short day in ``curation_run``, not read as a quiet
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
        ``objects_expected`` (hours in the window, times two tables)
        and ``objects_read`` (how many of those actually exist).
    """
    hours = expected_hours(window=window)
    expected = set(hours)
    client = (session or boto3.Session()).client('s3')
    read = sum(
        count_existing_partials(
            client=client, bucket=bucket, table=table, expected=expected,
        )
        for table in ('trip_stop', 'vehicle_position')
    )
    return len(hours) * 2, read


DIM_SCHEDULED_STOP_TIME_PREFIX: Final[str] = (
    'curated/dim_scheduled_stop_time/'
)


def latest_valid_from(
    *,
    client: Any,
    bucket: str,
    service_date: date,
) -> str | None:
    """Find the schedule snapshot in effect on a service date.

    Parameters
    ----------
    client : Any
        A boto3 S3 client.
    bucket : str
        Bucket holding the curated layer.
    service_date : date
        The service date whose schedule to resolve.

    Returns
    -------
    str | None
        The latest ``valid_from`` partition value at or before
        ``service_date``, or None if no snapshot qualifies.
    """
    pages = client.get_paginator('list_objects_v2').paginate(
        Bucket=bucket,
        Prefix=DIM_SCHEDULED_STOP_TIME_PREFIX,
        Delimiter='/',
    )
    candidates = (
        prefix['Prefix'].removeprefix(
            DIM_SCHEDULED_STOP_TIME_PREFIX,
        ).removeprefix('valid_from=').rstrip('/')
        for page in pages
        for prefix in page.get('CommonPrefixes', ())
    )
    eligible = [
        value for value in candidates
        if date.fromisoformat(value) <= service_date
    ]
    return max(eligible) if eligible else None


def load_scheduled_arrivals(
    *,
    connection: duckdb.DuckDBPyConnection,
    bucket: str,
    valid_from: str,
) -> dict[tuple[str, int], str]:
    """Read one schedule snapshot's arrival times.

    Parameters
    ----------
    connection : duckdb.DuckDBPyConnection
        Configured DuckDB connection.
    bucket : str
        Bucket holding the curated layer.
    valid_from : str
        The snapshot's ``valid_from`` partition value.

    Returns
    -------
    dict[tuple[str, int], str]
        Scheduled GTFS arrival time as written, keyed by
        ``(trip_id, stop_sequence)``.
    """
    source = (
        f's3://{bucket}/{DIM_SCHEDULED_STOP_TIME_PREFIX}'
        f'valid_from={valid_from}/data.parquet'
    )
    rows = connection.execute(
        'SELECT trip_id, stop_sequence, arrival_time '
        f"FROM read_parquet('{source}')",
    ).fetchall()
    return {
        (trip_id, stop_sequence): arrival_time
        for trip_id, stop_sequence, arrival_time in rows
    }


def resolve_scheduled_arrivals(
    *,
    connection: duckdb.DuckDBPyConnection,
    bucket: str,
    service_date: date,
    session: boto3.Session,
) -> dict[tuple[str, int], str]:
    """Load the schedule snapshot in effect for one service date.

    Parameters
    ----------
    connection : duckdb.DuckDBPyConnection
        Configured DuckDB connection.
    bucket : str
        Bucket holding the curated layer.
    service_date : date
        The Sydney service date being assembled.
    session : boto3.Session
        Session used to list dimension snapshots.

    Returns
    -------
    dict[tuple[str, int], str]
        Scheduled GTFS arrival time, keyed by
        ``(trip_id, stop_sequence)``. Empty when no snapshot exists at
        or before the service date.
    """
    valid_from = latest_valid_from(
        client=session.client('s3'), bucket=bucket,
        service_date=service_date,
    )
    if valid_from is None:
        return {}
    return load_scheduled_arrivals(
        connection=connection, bucket=bucket, valid_from=valid_from,
    )


def resolve_scheduled_arrival(
    *,
    service_date: str,
    trip_id: str,
    stop_sequence: int,
    scheduled_arrivals: dict[tuple[str, int], str],
) -> datetime | None:
    """Resolve one row's scheduled arrival instant.

    Converts a GTFS clock time - including hours past 24, a measured
    maximum of 30 - to a UTC instant using the trip's own service
    date, exactly as ``scheduled_instant`` does.

    Parameters
    ----------
    service_date : str
        The trip's own service date, as ``YYYYMMDD``.
    trip_id : str
        Trip identifier.
    stop_sequence : int
        Position of this call in the trip.
    scheduled_arrivals : dict[tuple[str, int], str]
        Scheduled arrival times from ``resolve_scheduled_arrivals``.

    Returns
    -------
    datetime | None
        UTC instant, or None when no schedule row matches - a genuine
        outcome, not an error, since the fact and dimension are
        curated on independent schedules.
    """
    gtfs_time = scheduled_arrivals.get((trip_id, stop_sequence))
    if gtfs_time is None:
        return None
    return scheduled_instant(start_date=service_date, gtfs_time=gtfs_time)


def add_scheduled_arrival(
    *,
    table: pa.Table,
    scheduled_arrivals: dict[tuple[str, int], str],
) -> pa.Table:
    """Append ``scheduled_arrival_utc`` to a merged trip-stop table.

    Done in Python rather than SQL: the >24:00 rollover needs
    ``scheduled_instant``'s Sydney-aware wall-clock arithmetic, which
    DuckDB's timestamp functions cannot express without reimplementing
    that already-tested logic a second time in SQL.

    Parameters
    ----------
    table : pa.Table
        Merged trip-stop rows, one per ``(service_date, trip_id,
        stop_id, stop_sequence)``.
    scheduled_arrivals : dict[tuple[str, int], str]
        Scheduled arrival times from ``resolve_scheduled_arrivals``.

    Returns
    -------
    pa.Table
        The same table with ``scheduled_arrival_utc`` appended.
    """
    column = [
        None if stop_sequence is None else resolve_scheduled_arrival(
            service_date=str(service_date),
            trip_id=str(trip_id),
            stop_sequence=int(stop_sequence),
            scheduled_arrivals=scheduled_arrivals,
        )
        for service_date, trip_id, stop_sequence in zip(
            table.column('service_date').to_pylist(),
            table.column('trip_id').to_pylist(),
            table.column('stop_sequence').to_pylist(),
        )
    ]
    return table.append_column(
        'scheduled_arrival_utc',
        pa.array(column, type=pa.timestamp('s', tz='UTC')),
    )


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
    glob = partial_glob(bucket=bucket, table='trip_stop')
    target = (
        f's3://{bucket}/curated/fact_trip_stop/'
        f'service_date={service_date:%Y-%m-%d}/data.parquet'
    )
    merged = connection.execute(
        TRIP_STOP_MERGE,
        {'partials': glob, 'service_date': f'{service_date:%Y%m%d}'},
    ).to_arrow_table()
    augmented = add_scheduled_arrival(
        table=merged,
        scheduled_arrivals=resolve_scheduled_arrivals(
            connection=connection,
            bucket=bucket,
            service_date=service_date,
            session=session or boto3.Session(),
        ),
    )
    connection.register('merged_trip_stops', augmented)
    connection.execute(
        f"COPY (SELECT * FROM merged_trip_stops) TO '{target}' "
        f'(FORMAT PARQUET, COMPRESSION SNAPPY)',
    )
    connection.unregister('merged_trip_stops')
    return count_parquet_rows(connection=connection, target=target)


def merge_positions(
    *,
    connection: duckdb.DuckDBPyConnection,
    bucket: str,
    service_date: date,
    window: tuple[datetime, datetime],
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

    Returns
    -------
    int
        Rows written.
    """
    glob = partial_glob(bucket=bucket, table='vehicle_position')
    target = (
        f's3://{bucket}/curated/fact_vehicle_position/'
        f'service_date={service_date:%Y-%m-%d}/data.parquet'
    )
    connection.execute(
        f"COPY ({POSITION_MERGE}) TO '{target}' "
        f'(FORMAT PARQUET, COMPRESSION SNAPPY)',
        {
            'partials': glob,
            'window_start': window[0],
            'window_end': window[1],
        },
    )
    return count_parquet_rows(connection=connection, target=target)


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
    rows_written: tuple[int, int, int],
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
    rows_written : tuple[int, int, int]
        Rows written to ``fact_collector_run``, ``fact_trip_stop`` and
        ``fact_vehicle_position``, respectively.
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


def warn_on_short_day(*, coverage: tuple[int, int]) -> None:
    """Log a warning when fewer partials exist than the window implies.

    Never raises: partial data is better than none, and the merger
    must stay safely re-runnable rather than failing a whole day over
    one missing hour.

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
        override.
    context : LambdaContext
        Lambda context, used for the invocation id.
    endpoint : str | None
        Test-only S3 endpoint override, forwarded to ``configure`` and
        ``merge_collector_run``. Lambda invokes with two positional
        arguments only, so this keyword-only default never affects
        production.

    Returns
    -------
    CurationRecord
        The audit record written for this run.
    """
    bucket = os.environ['BUCKET_NAME']
    started = datetime.now(tz=UTC)
    service_date = target_service_date(event=event, now=started)
    window = merge_window(service_date=service_date)
    session = boto3.Session()
    connection = duckdb.connect()
    configure(connection=connection, endpoint=endpoint)
    collector_rows = merge_collector_run(
        bucket=bucket, service_date=service_date, endpoint=endpoint,
    )
    trip_rows = merge_trip_stops(
        connection=connection, bucket=bucket, service_date=service_date,
        session=session,
    )
    position_rows = merge_positions(
        connection=connection, bucket=bucket,
        service_date=service_date, window=window,
    )
    coverage = count_partial_coverage(
        bucket=bucket, window=window, session=session,
    )
    warn_on_short_day(coverage=coverage)
    record = build_record(
        context=context, started=started, service_date=service_date,
        rows_written=(collector_rows, trip_rows, position_rows),
        partial_coverage=coverage,
    )
    CurationRepository(bucket=bucket, session=session).put_record(
        record=record,
    )
    return record
