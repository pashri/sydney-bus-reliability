"""Daily assembly of hourly partials into service-day facts.

Runs at about 04:00 Sydney, not midnight. A trip can belong to one
service day while running past midnight, as late as hour 30, i.e.
06:00 the next calendar day. Merging at midnight would finalise a
service date before its last trips had finished reporting.

The window of partials to read is computed timezone-aware, since
Sydney observes daylight saving and the UTC offset changes mid-season.
"""

import os
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any, Final, TypedDict

import boto3
import duckdb
from aws_lambda_powertools import Logger
from aws_lambda_powertools.utilities.typing import LambdaContext

from common.curation import CurationRepository
from common.process import peak_rss_mb
from common.service_day import merge_window, service_date_for
from common.types_ import CurationJob, CurationRecord
from merger.merge_sql import POSITION_MERGE, build_trip_stop_query

logger = Logger()

PARTIAL_PREFIX: Final[str] = 'curated/_partial'

HTTPFS_EXTENSION: Final[Path] = Path(
    '/opt/python/duckdb_extensions/httpfs.duckdb_extension',
)
"""Where ``scripts/build_duckdb_layer.sh`` puts httpfs in the layer."""

DUCKDB_THREADS: Final[int] = 2
"""Worker threads.

The function's memory allocation buys it about one vCPU, and DuckDB
sizes its own pool from the machine it detects rather than from that.
"""

DUCKDB_MEMORY_LIMIT: Final[str] = '1500MB'
"""Headroom below the function's allocation, so DuckDB spills first."""

DUCKDB_TEMP_DIRECTORY: Final[str] = '/tmp'
"""Where spilled data goes. The only writable path on Lambda."""

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
        Override S3 endpoint, host[:port] only. A test seam, never
        set in production. DuckDB's httpfs opens its own sockets, so
        ``mock_aws()`` cannot intercept it and tests must point it at
        a real local moto server instead.
    """
    load_extensions(connection=connection)
    apply_limits(connection=connection)
    create_s3_secret(connection=connection, endpoint=endpoint)


def load_extensions(*, connection: duckdb.DuckDBPyConnection) -> None:
    """Load the extensions the merge SQL needs.

    ``httpfs`` backs every ``s3://`` read and write. ``icu`` backs
    ``AT TIME ZONE`` with a named zone, which resolves
    ``scheduled_arrival_utc``; it is compiled into the DuckDB wheel and
    loads without a download.

    The layer ships httpfs so that a run never depends on DuckDB's
    extension repository being reachable. Off Lambda the layer is
    absent, and httpfs is fetched from that repository instead.

    Parameters
    ----------
    connection : duckdb.DuckDBPyConnection
        Connection to load into.
    """
    if HTTPFS_EXTENSION.exists():
        connection.execute('SET autoinstall_known_extensions = false')
        connection.execute('SET autoload_known_extensions = false')
        connection.execute(f"LOAD '{HTTPFS_EXTENSION}';")
    else:
        connection.execute('INSTALL httpfs; LOAD httpfs;')
    connection.execute('LOAD icu;')


def apply_limits(*, connection: duckdb.DuckDBPyConnection) -> None:
    """Bound DuckDB's threads and memory to the function's allocation.

    DuckDB sizes both from the machine it detects, which in a container
    can be the host rather than the slice the function was given. Left
    alone it can run more workers than there is CPU for, and believe it
    has memory it does not have, so it never spills before the runtime
    kills the process.

    Parameters
    ----------
    connection : duckdb.DuckDBPyConnection
        Connection to bound.
    """
    connection.execute(f'SET threads = {DUCKDB_THREADS}')
    connection.execute(f"SET memory_limit = '{DUCKDB_MEMORY_LIMIT}'")
    connection.execute(
        f"SET temp_directory = '{DUCKDB_TEMP_DIRECTORY}'",
    )
    logger.info(
        'DuckDB limits applied',
        extra=effective_limits(connection=connection),
    )


def effective_limits(
    *,
    connection: duckdb.DuckDBPyConnection,
) -> dict[str, str]:
    """Read back the limits DuckDB is actually running under.

    Parameters
    ----------
    connection : duckdb.DuckDBPyConnection
        Connection to interrogate.

    Returns
    -------
    dict[str, str]
        Each setting's name and its current value.
    """
    settings = ('threads', 'memory_limit', 'temp_directory')
    return {
        name: setting_value(connection=connection, name=name)
        for name in settings
    }


def setting_value(
    *,
    connection: duckdb.DuckDBPyConnection,
    name: str,
) -> str:
    """Read one DuckDB setting's current value.

    Parameters
    ----------
    connection : duckdb.DuckDBPyConnection
        Connection to interrogate.
    name : str
        Setting to read.

    Returns
    -------
    str
        The setting's value, or an empty string if it has none.
    """
    result = connection.execute(
        f"SELECT current_setting('{name}')",
    ).fetchone()
    return str(result[0]) if result else ''


def create_s3_secret(
    *,
    connection: duckdb.DuckDBPyConnection,
    endpoint: str | None = None,
) -> None:
    """Give a connection credentials for S3.

    Parameters
    ----------
    connection : duckdb.DuckDBPyConnection
        Connection to configure.
    endpoint : str | None
        Override S3 endpoint, host[:port] only.
    """
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

    Written to ``fact_collector_run``, a separate prefix. The source
    JSONL under ``collector_run`` is left in place, because the
    collector keeps appending to it and ``check_collection.py`` reads
    it directly.

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

    One glob across all UTC dates, not one per date in the merge
    window. DuckDB's ``read_parquet`` errors on a glob list entry
    matching zero files, which a missing hour would trigger.
    Narrowing to the service day is left to each merge query's own
    ``WHERE`` clause.

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


def resolve_dim_source(
    *,
    bucket: str,
    service_date: date,
    session: boto3.Session,
) -> str | None:
    """Locate the schedule snapshot in effect for one service date.

    Lists S3 prefixes only. No dimension rows are fetched into
    Python, so no ``TIMESTAMPTZ`` value crosses the DuckDB boundary.
    The join happens entirely in SQL, in ``build_trip_stop_query``.

    Parameters
    ----------
    bucket : str
        Bucket holding the curated layer.
    service_date : date
        The Sydney service date being assembled.
    session : boto3.Session
        Session used to list dimension snapshots.

    Returns
    -------
    str | None
        S3 path to the snapshot's Parquet object, or None when no
        snapshot exists at or before the service date.
    """
    valid_from = latest_valid_from(
        client=session.client('s3'), bucket=bucket,
        service_date=service_date,
    )
    if valid_from is None:
        return None
    return (
        f's3://{bucket}/{DIM_SCHEDULED_STOP_TIME_PREFIX}'
        f'valid_from={valid_from}/data.parquet'
    )


def merge_trip_stops(
    *,
    connection: duckdb.DuckDBPyConnection,
    bucket: str,
    service_date: date,
    window: tuple[datetime, datetime],
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
    window : tuple[datetime, datetime]
        UTC range of partials to read, used to bound the partitions
        scanned.
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
    dim_source = resolve_dim_source(
        bucket=bucket, service_date=service_date,
        session=session or boto3.Session(),
    )
    query = build_trip_stop_query(dim_source=dim_source)
    dt_from, dt_to = partition_bounds(window=window)
    connection.execute(
        f"COPY ({query}) TO '{target}' "
        f'(FORMAT PARQUET, COMPRESSION SNAPPY)',
        {
            'partials': glob,
            'service_date': f'{service_date:%Y%m%d}',
            'dt_from': dt_from,
            'dt_to': dt_to,
        },
    )
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
    dt_from, dt_to = partition_bounds(window=window)
    connection.execute(
        f"COPY ({POSITION_MERGE}) TO '{target}' "
        f'(FORMAT PARQUET, COMPRESSION SNAPPY)',
        {
            'partials': glob,
            'window_start': window[0],
            'window_end': window[1],
            'dt_from': dt_from,
            'dt_to': dt_to,
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
        override.
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
        window=window, session=session,
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
