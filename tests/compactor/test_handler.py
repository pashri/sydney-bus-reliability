"""Tests for the hourly compactor handler."""

import gzip
import os
from datetime import UTC, datetime

import boto3
import pytest
from google.transit import gtfs_realtime_pb2

from compactor.handler import handler, partial_key, target_hour

FETCHED_POSITION: datetime = datetime(2026, 9, 16, 21, 0, 4, tzinfo=UTC)
FETCHED_TRIP: datetime = datetime(2026, 9, 16, 21, 0, 34, tzinfo=UTC)
SCHEDULED = gtfs_realtime_pb2.TripUpdate.StopTimeUpdate.SCHEDULED


class _Context:
    """Minimal Lambda context for Powertools."""

    function_name = 'sydney-bus-reliability-compactor'
    memory_limit_in_mb = 1024
    invoked_function_arn = (
        'arn:aws:lambda:ap-southeast-2:000000000000:function:test'
    )
    aws_request_id = 'test-request-id'


def build_position_feed() -> gtfs_realtime_pb2.FeedMessage:
    """Build a minimal FeedMessage of one vehicle position.

    Returns
    -------
    gtfs_realtime_pb2.FeedMessage
        A populated FeedMessage.
    """
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.header.gtfs_realtime_version = '1.0'
    entity = feed.entity.add()
    entity.id = '8183_a'
    entity.vehicle.vehicle.id = '8183_a'
    entity.vehicle.vehicle.label = '8183'
    entity.vehicle.trip.trip_id = '1012281'
    entity.vehicle.trip.route_id = '2447_160'
    entity.vehicle.timestamp = 1789592400
    entity.vehicle.position.latitude = -33.8
    entity.vehicle.position.longitude = 151.2
    return feed


def build_trip_feed() -> gtfs_realtime_pb2.FeedMessage:
    """Build a minimal FeedMessage of one trip update.

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
    stop = update.stop_time_update.add()
    stop.stop_id = '200013'
    stop.stop_sequence = 1
    stop.schedule_relationship = SCHEDULED
    stop.arrival.time = 1789592460
    stop.arrival.delay = 30
    return feed


def test_target_hour_defaults_to_the_hour_that_just_closed() -> None:
    """At 22:10 the compactor works on the 21:00 hour."""
    now = datetime(2026, 9, 16, 22, 10, tzinfo=UTC)
    assert target_hour(event={}, now=now) == datetime(
        2026, 9, 16, 21, 0, tzinfo=UTC,
    )


def test_target_hour_accepts_explicit_backfill() -> None:
    """An explicit hour in the event enables cheap backfill."""
    assert target_hour(
        event={'hour': '2026-09-16T05:00:00+00:00'},
        now=datetime(2026, 9, 16, 22, 10, tzinfo=UTC),
    ) == datetime(2026, 9, 16, 5, 0, tzinfo=UTC)


def test_partial_key_uses_underscore_prefix() -> None:
    """Partials sit under _partial/ so DuckDB globs skip them."""
    key = partial_key(
        table='trip_stop', hour=datetime(2026, 9, 16, 21, tzinfo=UTC),
    )
    assert key == (
        'curated/_partial/trip_stop/dt=2026-09-16/hour=21/data.parquet'
    )


def _put_one_hour_of_raw_objects(*, bucket: str) -> None:
    """Store one position and one trip-update object in the test hour.

    Parameters
    ----------
    bucket : str
        Destination bucket.
    """
    client = boto3.client('s3')
    client.put_object(
        Bucket=bucket,
        Key='raw/vehiclepos/dt=2026-09-16/hour=21/210004.pb.gz',
        Body=gzip.compress(build_position_feed().SerializeToString()),
    )
    client.put_object(
        Bucket=bucket,
        Key='raw/tripupdates/dt=2026-09-16/hour=21/210034.pb.gz',
        Body=gzip.compress(build_trip_feed().SerializeToString()),
    )


def test_handler_writes_both_partials(_bucket: str) -> None:
    """One hour produces one positions partial and one trip partial."""
    _put_one_hour_of_raw_objects(bucket=_bucket)
    os.environ['BUCKET_NAME'] = _bucket
    record = handler(
        {'hour': '2026-09-16T21:00:00+00:00'}, _Context(),
    )
    assert record['rows_out'] > 0
    assert record['objects_read'] == 2


def test_handler_records_shortfall_without_raising(_bucket: str) -> None:
    """A partial hour is recorded, not raised - gaps are real."""
    _put_one_hour_of_raw_objects(bucket=_bucket)
    os.environ['BUCKET_NAME'] = _bucket
    record = handler(
        {'hour': '2026-09-16T21:00:00+00:00'}, _Context(),
    )
    assert record['objects_expected'] == 420
    assert record['objects_read'] == 2
    assert record['error'] is None


def test_handler_raises_on_completely_empty_hour(_bucket: str) -> None:
    """Zero objects is a failure, unlike a shortfall."""
    os.environ['BUCKET_NAME'] = _bucket
    with pytest.raises(RuntimeError):
        handler({'hour': '2026-09-16T21:00:00+00:00'}, _Context())
