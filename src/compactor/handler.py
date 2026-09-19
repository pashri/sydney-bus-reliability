"""Hourly compaction of one UTC hour of raw feed objects.

Ordering is load-bearing. Trip updates are processed and their reducer
released *before* vehicle positions begin: measured peak RSS is
1,527 MB that way against 4,007 MB when both are held, and the
streaming version is marginally *faster*, so there is no trade-off to
weigh. Getting this wrong quadruples the memory tier - and therefore
the bill - every hour of every day, while producing identical output.
"""

import os
import resource
import sys
from collections import Counter
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from typing import Any, Final

import boto3
from aws_lambda_powertools import Logger
from aws_lambda_powertools.utilities.typing import LambdaContext

from src.common.curation import CurationRepository
from src.common.feed_decode import decode_feed
from src.common.parquet import ParquetRepository
from src.common.raw_read import EXPECTED_OBJECTS, RawReader
from src.common.types_ import CurationJob, CurationRecord, Feed
from src.compactor.positions import (
    POSITION_SCHEMA,
    PositionDeduper,
    position_batches,
)
from src.compactor.trip_updates import TRIP_STOP_SCHEMA, TripStopReducer

logger = Logger()

PARTIAL_PREFIX: Final[str] = 'curated/_partial'


def target_hour(*, event: dict[str, Any], now: datetime) -> datetime:
    """Choose which UTC hour to compact.

    Parameters
    ----------
    event : dict[str, Any]
        EventBridge event, optionally carrying an ``hour`` override
        for backfill.
    now : datetime
        Current UTC time.

    Returns
    -------
    datetime
        Start of the hour to process.
    """
    override = event.get('hour')
    if override:
        return datetime.fromisoformat(override)
    return (now - timedelta(hours=1)).replace(
        minute=0, second=0, microsecond=0,
    )


def partial_key(*, table: str, hour: datetime) -> str:
    """Build the S3 key for one hourly partial.

    Parameters
    ----------
    table : str
        Either ``vehicle_position`` or ``trip_stop``.
    hour : datetime
        Start of the hour.

    Returns
    -------
    str
        S3 key under the ``_partial`` prefix, whose leading underscore
        signals to anyone globbing ``curated/`` that this is an
        implementation detail rather than a published table.
    """
    return (
        f'{PARTIAL_PREFIX}/{table}/dt={hour:%Y-%m-%d}/'
        f'hour={hour:%H}/data.parquet'
    )


def compact_trip_updates(
    *,
    reader: RawReader,
    parquet: ParquetRepository,
    hour: datetime,
) -> tuple[int, int, int]:
    """Reduce one hour of trip updates and write the partial.

    Parameters
    ----------
    reader : RawReader
        Source of raw objects.
    parquet : ParquetRepository
        Destination for the partial.
    hour : datetime
        Start of the hour.

    Returns
    -------
    tuple[int, int, int]
        Objects read, real observations seen, and rows written.
    """
    reducer = TripStopReducer()
    objects = 0
    for raw in reader.stream_hour(feed=Feed.TRIP_UPDATES, hour=hour):
        reducer.add(
            feed=decode_feed(payload=raw.payload),
            fetched_at=raw.fetched_at,
        )
        objects += 1
    rows = parquet.put_batches(
        key=partial_key(table='trip_stop', hour=hour),
        schema=TRIP_STOP_SCHEMA,
        batches=reducer.batches(),
    )
    return objects, reducer.real_observations, rows


def position_records(
    *,
    reader: RawReader,
    hour: datetime,
    deduper: PositionDeduper,
    counted: Counter[str],
) -> Iterator[dict[str, Any]]:
    """Stream deduplicated position rows across every poll in the hour.

    Rows are never accumulated: the deduper retains only the ~256-byte
    key per distinct observation, against 778-1,048 bytes for a row
    dict. At the peak hour's 1,086,002 rows that is the difference
    between a 1 GB and a 2 GB Lambda tier, paid on 24 invocations a
    day forever.

    Parameters
    ----------
    reader : RawReader
        Source of raw objects.
    hour : datetime
        Start of the hour.
    deduper : PositionDeduper
        Cross-poll deduplicator, mutated as objects are read.
    counted : Counter[str]
        Shared counter incremented once per object read.

    Yields
    ------
    dict[str, Any]
        One accepted vehicle-position record.
    """
    for raw in reader.stream_hour(feed=Feed.VEHICLE_POSITIONS, hour=hour):
        counted['objects'] += 1
        yield from deduper.rows(
            feed=decode_feed(payload=raw.payload),
            fetched_at=raw.fetched_at,
        )


def compact_positions(
    *,
    reader: RawReader,
    parquet: ParquetRepository,
    hour: datetime,
) -> tuple[int, int, PositionDeduper]:
    """Deduplicate one hour of positions and write the partial.

    Parameters
    ----------
    reader : RawReader
        Source of raw objects.
    parquet : ParquetRepository
        Destination for the partial.
    hour : datetime
        Start of the hour.

    Returns
    -------
    tuple[int, int, PositionDeduper]
        Objects read, rows written, and the deduper for its counters.
    """
    deduper = PositionDeduper()
    counted: Counter[str] = Counter()
    rows = parquet.put_batches(
        key=partial_key(table='vehicle_position', hour=hour),
        schema=POSITION_SCHEMA,
        batches=position_batches(records=position_records(
            reader=reader, hour=hour, deduper=deduper, counted=counted,
        )),
    )
    return counted['objects'], rows, deduper


def build_record(
    *,
    context: LambdaContext,
    started: datetime,
    hour: datetime,
    trip: tuple[int, int, int],
    position: tuple[int, int, PositionDeduper],
) -> CurationRecord:
    """Assemble one invocation's audit record.

    Parameters
    ----------
    context : LambdaContext
        Lambda context, used for the invocation id.
    started : datetime
        UTC instant the invocation began.
    hour : datetime
        Start of the compacted hour.
    trip : tuple[int, int, int]
        Objects read, real observations, and rows written for trip
        updates.
    position : tuple[int, int, PositionDeduper]
        Objects read, rows written, and the deduper for positions.

    Returns
    -------
    CurationRecord
        The audit record for this run.
    """
    trip_objects, observations, trip_rows = trip
    position_objects, position_rows, deduper = position
    return {
        'job': CurationJob.COMPACTOR.value,
        'invocation_id': context.aws_request_id,
        'started_at_utc': started.isoformat(),
        'finished_at_utc': datetime.now(tz=UTC).isoformat(),
        'partition': f'{hour:%Y-%m-%dT%H}',
        'objects_expected': sum(EXPECTED_OBJECTS.values()),
        'objects_read': trip_objects + position_objects,
        'rows_in': observations,
        'rows_out': trip_rows + position_rows,
        'dupes_collapsed': deduper.collapsed,
        'dupes_differing_position': deduper.differing_position,
        'unjoined_route_ids': 0,
        'unjoined_trip_ids': 0,
        'unjoined_stop_ids': 0,
        'peak_rss_mb': peak_rss_mb(),
        'error': None,
    }


def peak_rss_mb() -> int:
    """Report this process's peak resident set size in megabytes.

    Recorded on every run because the memory envelope cost Phase 1 the
    most time, and because the streaming requirement above has no
    guard other than measurement.

    Returns
    -------
    int
        Peak RSS in MB.

    Notes
    -----
    Linux reports ``ru_maxrss`` in kilobytes; macOS reports it in
    bytes. Dividing unconditionally by 1024 is roughly 1000x wrong on
    one of the two platforms, so the divisor is chosen by
    ``sys.platform``.
    """
    usage = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    if sys.platform == 'darwin':
        return usage // 1024**2
    return usage // 1024


@logger.inject_lambda_context
def handler(
    event: dict[str, Any],
    context: LambdaContext,
) -> CurationRecord:
    """Compact one UTC hour of raw feed objects into partials.

    Parameters
    ----------
    event : dict[str, Any]
        EventBridge event, optionally carrying an ``hour`` override.
    context : LambdaContext
        Lambda context, used for the invocation id.

    Returns
    -------
    CurationRecord
        The audit record written for this run.

    Raises
    ------
    RuntimeError
        If the hour contains no objects at all, which means either a
        total collection failure or a misaddressed hour. A shortfall
        short of zero is recorded on the returned record instead: one
        real gap already exists (20:09 UTC 16 September, lost to an
        OOM), and raising on every partial hour would page on noise.
    """
    session = boto3.Session()
    bucket = os.environ['BUCKET_NAME']
    started = datetime.now(tz=UTC)
    hour = target_hour(event=event, now=started)
    reader = RawReader(bucket=bucket, session=session)
    parquet = ParquetRepository(bucket=bucket, session=session)
    # Trip updates first, and the reducer released before positions
    # begin. See this module's docstring: this ordering is the
    # difference between a 1,527 MB and a 4,007 MB peak.
    trip = compact_trip_updates(reader=reader, parquet=parquet, hour=hour)
    position = compact_positions(
        reader=reader, parquet=parquet, hour=hour,
    )
    if not trip[0] + position[0]:
        raise RuntimeError(f'no raw objects for hour {hour:%Y-%m-%dT%H}')
    record = build_record(
        context=context, started=started, hour=hour,
        trip=trip, position=position,
    )
    CurationRepository(bucket=bucket, session=session).put_record(
        record=record,
    )
    return record
