"""End-to-end seam test: compactor reduction through ParquetRepository
into the merger's SQL.

Compactor tests write their fixtures through ``ParquetRepository``.
Merger tests build fixtures directly with
``pyarrow.parquet.write_table``. Each side is therefore only ever
tested against a fixture it built itself, so a divergence introduced
by ``put_batches``/``write_buffer`` - a dropped batch, a schema
coercion, a compression setting DuckDB cannot read back - would be
invisible to both suites. This test runs one small feed through the
real reduction, writes the partial through the real repository, and
reads it back through the real merge SQL.
"""

from datetime import UTC, datetime

import duckdb
from google.transit import gtfs_realtime_pb2

from common.parquet import ParquetRepository
from compactor.trip_updates import TRIP_STOP_SCHEMA, TripStopReducer
from merger.handler import configure
from merger.merge_sql import TRIP_STOP_MERGE

FETCHED: datetime = datetime(2026, 9, 16, 21, 0, 34, tzinfo=UTC)
SCHEDULED = gtfs_realtime_pb2.TripUpdate.StopTimeUpdate.SCHEDULED


def build_feed() -> gtfs_realtime_pb2.FeedMessage:
    """Build a FeedMessage holding one real trip-stop observation.

    Returns
    -------
    gtfs_realtime_pb2.FeedMessage
        A populated FeedMessage.
    """
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.header.gtfs_realtime_version = '1.0'
    entity = feed.entity.add()
    entity.id = 'tu-1'
    update = entity.trip_update
    update.trip.trip_id = '1012281'
    update.trip.route_id = '2447_160'
    update.trip.start_date = '20260917'
    update.vehicle.id = '8183_a'
    update.timestamp = 1789592670
    stop = update.stop_time_update.add()
    stop.stop_id = '200013'
    stop.stop_sequence = 3
    stop.schedule_relationship = SCHEDULED
    stop.arrival.time = 1789592700
    stop.arrival.delay = 60
    return feed


def test_compactor_output_round_trips_into_the_merger(
    _bucket: str, _s3_endpoint: str,
) -> None:
    """A real reduction, written by the real repository, merges cleanly.

    Exercises ``put_batches``/``write_buffer`` on the write side and
    the real ``TRIP_STOP_MERGE`` SQL on the read side, over a local
    moto bucket rather than either suite's own hand-built fixture.
    """
    reducer = TripStopReducer()
    reducer.add(feed=build_feed(), fetched_at=FETCHED)
    key = 'curated/_partial/trip_stop/dt=2026-09-16/hour=21/data.parquet'
    repository = ParquetRepository(bucket=_bucket)
    written = repository.put_batches(
        key=key, schema=TRIP_STOP_SCHEMA, batches=reducer.batches(),
    )
    assert written == 1

    connection = duckdb.connect()
    configure(connection=connection, endpoint=_s3_endpoint)
    glob = (
        f's3://{_bucket}/curated/_partial/trip_stop/'
        f'dt=*/hour=*/data.parquet'
    )
    result = connection.execute(
        TRIP_STOP_MERGE,
        {'partials': glob, 'service_date': '20260917'},
    ).fetchall()
    columns = [d[0] for d in connection.description]
    merged = dict(zip(columns, result[0]))
    assert merged['trip_id'] == '1012281'
    assert merged['stop_sequence'] == 3
    assert merged['delay_s'] == 60
    assert merged['is_reliable']
